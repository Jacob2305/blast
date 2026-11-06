/*
 * receiver.c - BLAST file receiver.
 *
 * Threads:
 *   network thread (main) : receives datagrams, marks the per-blast bitmap,
 *                           copies records into the current blast buffer,
 *                           answers IS_BLAST_OVER with REC_MISS.
 *   writer thread         : writes completed blast buffers to disk, so a slow
 *                           disk does not stall packet reception. A small pool
 *                           of blast buffers (-B) decouples the two threads;
 *                           the network thread only blocks if ALL buffers are
 *                           waiting for the disk (backpressure).
 *
 * Receiver state machine:
 *   IDLE      waiting for FILE_HDR
 *   ACK_SENT  FILE_HDR_ACK sent, waiting for first DATA
 *   RECEIVING marking bitmap as DATA arrives for the current blast
 *             (IS_BLAST_OVER -> REPORTING: send REC_MISS; if empty, hand the
 *              blast to the writer and move on to the next blast)
 *   LINGERING all blasts complete; keep answering retransmitted
 *             IS_BLAST_OVER (empty REC_MISS) and FIN (FIN_ACK) for a while
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common.h"
#include "garbler.h"

#define NBUF_MAX 16

static int verbose;
#define VLOG(...) do { if (verbose) fprintf(stderr, __VA_ARGS__); } while (0)

enum rstate { R_IDLE, R_ACK_SENT, R_RECEIVING, R_LINGERING };

/* ------------------------------------------------------------------ */
/* buffer pool + writer thread                                          */
/* ------------------------------------------------------------------ */
typedef struct { uint8_t *buf; uint64_t offset; size_t nbytes; } job_t;

static pthread_mutex_t mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t cv_free = PTHREAD_COND_INITIALIZER;
static pthread_cond_t cv_job = PTHREAD_COND_INITIALIZER;
static uint8_t *free_stack[NBUF_MAX];
static int nfree, nbuf = 3;
static job_t jobs[NBUF_MAX + 1];
static int jhead, jtail, jcount;
static int shutting;
static int out_fd = -1;
static int write_err;
static unsigned disk_delay_us;     /* simulate a slow disk (testing) */
static uint64_t buf_waits;

static uint8_t *acquire_buf(void)
{
    pthread_mutex_lock(&mu);
    if (nfree == 0) buf_waits++;
    while (nfree == 0) pthread_cond_wait(&cv_free, &mu);
    uint8_t *b = free_stack[--nfree];
    pthread_mutex_unlock(&mu);
    return b;
}

static void submit_job(job_t j)
{
    pthread_mutex_lock(&mu);
    jobs[jtail] = j;
    jtail = (jtail + 1) % (NBUF_MAX + 1);
    jcount++;
    pthread_cond_signal(&cv_job);
    pthread_mutex_unlock(&mu);
}

static void *writer_thread(void *arg)
{
    (void)arg;
    for (;;) {
        pthread_mutex_lock(&mu);
        while (jcount == 0 && !shutting) pthread_cond_wait(&cv_job, &mu);
        if (jcount == 0 && shutting) { pthread_mutex_unlock(&mu); break; }
        job_t j = jobs[jhead];
        jhead = (jhead + 1) % (NBUF_MAX + 1);
        jcount--;
        pthread_mutex_unlock(&mu);

        if (disk_delay_us) usleep(disk_delay_us);
        size_t done = 0;
        while (done < j.nbytes) {
            ssize_t w = pwrite(out_fd, j.buf + done, j.nbytes - done,
                               (off_t)(j.offset + done));
            if (w < 0) {
                if (errno == EINTR) continue;
                perror("pwrite");
                write_err = 1;
                break;
            }
            done += (size_t)w;
        }

        pthread_mutex_lock(&mu);
        free_stack[nfree++] = j.buf;
        pthread_cond_signal(&cv_free);
        pthread_mutex_unlock(&mu);
    }
    return NULL;
}

/* ------------------------------------------------------------------ */
/* protocol state                                                       */
/* ------------------------------------------------------------------ */
static int sock;
static struct sockaddr_in peer;
static int have_peer;

static uint64_t file_size, total_records;
static uint32_t rs, M, N, nblasts;

static enum rstate state = R_IDLE;
static uint32_t cur_blast;
static uint64_t cur_mst;
static uint32_t cur_nrec, cur_count;
static uint8_t *cur_buf;
static uint8_t *bitmap;

static struct {
    uint64_t pkts_recv, data_pkts, dup_records, stale_pkts;
    uint64_t miss_sent, miss_truncated;
} S;

#define BIT_GET(i) (bitmap[(i) >> 3] & (1u << ((i) & 7)))
#define BIT_SET(i) (bitmap[(i) >> 3] |= (uint8_t)(1u << ((i) & 7)))

static void tx(const void *b, size_t n)
{
    g_sendto(sock, b, n, (struct sockaddr *)&peer, sizeof peer);
}

static void begin_blast(uint32_t b)
{
    cur_buf = acquire_buf();
    cur_mst = (uint64_t)b * M;
    uint64_t left = total_records - cur_mst;
    cur_nrec = (uint32_t)(left < M ? left : M);
    memset(bitmap, 0, ((size_t)M + 7) / 8);
    cur_count = 0;
}

static void complete_blast(void)
{
    job_t j;
    j.buf = cur_buf;
    j.offset = cur_mst * rs;
    uint64_t want = (uint64_t)cur_nrec * rs;
    uint64_t avail = file_size - j.offset;
    j.nbytes = (size_t)(want < avail ? want : avail);   /* drop zero padding */
    submit_job(j);
    cur_buf = NULL;
    cur_blast++;
    if (cur_blast < nblasts) begin_blast(cur_blast);
    else state = R_LINGERING;
}

static void send_rec_miss(uint32_t blast, uint32_t round, const range_t *r, int nr)
{
    uint8_t pkt[11 + 8 * MAX_MISS_RANGES];
    pkt[0] = PKT_REC_MISS;
    put32(pkt + 1, blast);
    put32(pkt + 5, round);
    put16(pkt + 9, (uint16_t)nr);
    for (int i = 0; i < nr; i++) {
        put32(pkt + 11 + 8 * i, r[i].st);
        put32(pkt + 15 + 8 * i, r[i].end);
    }
    tx(pkt, 11 + 8 * (size_t)nr);
    S.miss_sent++;
}

static void handle_file_hdr(const uint8_t *b, ssize_t n, const struct sockaddr_in *from,
                            const char *outdir)
{
    if (n < 23) return;
    uint64_t fsz = get64(b + 1);
    uint32_t rsz = get32(b + 9), m = get32(b + 13), nn = get32(b + 17);
    uint16_t nl = get16(b + 21);
    if (nl == 0 || nl > MAX_FNAME || n < 23 + (ssize_t)nl) return;

    uint8_t ack[14];
    ack[0] = PKT_FILE_HDR_ACK;

    if (have_peer) {
        /* duplicate FILE_HDR (our ACK was lost): repeat the ACK */
        ack[1] = 0;
        put32(ack + 2, rs);
        put32(ack + 6, M);
        put32(ack + 10, N);
        tx(ack, sizeof ack);
        return;
    }

    peer = *from;
    if (!valid_record_size(rsz) || m < 1 || nn < 1) {
        ack[1] = 1;
        put32(ack + 2, rsz);
        put32(ack + 6, m);
        put32(ack + 10, nn);
        tx(ack, sizeof ack);
        return;
    }
    if (m > MAX_M) m = MAX_M;                 /* receiver may lower M */
    if (nn > MAX_SEGS_PER_PKT) nn = MAX_SEGS_PER_PKT;

    /* sanitize file name: keep only the last path component */
    char name[MAX_FNAME + 1];
    memcpy(name, b + 23, nl);
    name[nl] = '\0';
    const char *base = strrchr(name, '/');
    base = base ? base + 1 : name;
    if (!*base || !strcmp(base, ".") || !strcmp(base, "..")) base = "received.bin";

    char path[1024];
    snprintf(path, sizeof path, "%s/%s", outdir, base);
    out_fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (out_fd < 0) {
        perror("open output");
        ack[1] = 1;
        put32(ack + 2, rsz);
        put32(ack + 6, m);
        put32(ack + 10, nn);
        tx(ack, sizeof ack);
        return;
    }
    if (fsz > 0 && ftruncate(out_fd, (off_t)fsz) < 0) perror("ftruncate");

    file_size = fsz;
    rs = rsz;
    M = m;
    N = nn;
    total_records = (fsz + rs - 1) / rs;
    nblasts = (uint32_t)((total_records + M - 1) / M);
    have_peer = 1;

    bitmap = calloc(((size_t)M + 7) / 8, 1);
    for (int i = 0; i < nbuf; i++) {
        free_stack[i] = malloc((size_t)M * rs);
        if (!free_stack[i]) { fprintf(stderr, "out of memory\n"); exit(1); }
    }
    nfree = nbuf;

    ack[1] = 0;
    put32(ack + 2, rs);
    put32(ack + 6, M);
    put32(ack + 10, N);
    tx(ack, sizeof ack);

    VLOG("receiver: FILE_HDR ok: %s, %llu bytes, rs=%u M=%u N=%u, %u blasts\n",
         path, ULL(file_size), rs, M, N, nblasts);

    if (nblasts == 0) state = R_LINGERING;
    else { begin_blast(0); state = R_ACK_SENT; }
}

static void handle_data(const uint8_t *b, ssize_t n)
{
    S.data_pkts++;
    if (state != R_ACK_SENT && state != R_RECEIVING) { S.stale_pkts++; return; }
    if (n < DATA_HDR_LEN) return;
    uint32_t blast = get32(b + 1), mst = get32(b + 5), mfin = get32(b + 9);
    int ns = get16(b + 13), nrec = get16(b + 15);
    if (blast != cur_blast) { S.stale_pkts++; return; }
    if (mst != cur_mst || (uint64_t)mfin != cur_mst + cur_nrec - 1) return;
    if (ns < 1 || ns > MAX_SEGS_PER_PKT || nrec < 1 || nrec > MAX_REC_PER_PKT) return;
    if (n != DATA_HDR_LEN + 8 * ns + (ssize_t)nrec * (ssize_t)rs) return;

    int sum = 0;
    for (int i = 0; i < ns; i++) {
        uint32_t st = get32(b + DATA_HDR_LEN + 8 * i);
        uint32_t en = get32(b + DATA_HDR_LEN + 8 * i + 4);
        if (en < st || st < cur_mst || en >= cur_mst + cur_nrec) return;
        sum += (int)(en - st + 1);
    }
    if (sum != nrec) return;

    const uint8_t *payload = b + DATA_HDR_LEN + 8 * ns;
    for (int i = 0; i < ns; i++) {
        uint32_t st = get32(b + DATA_HDR_LEN + 8 * i);
        uint32_t en = get32(b + DATA_HDR_LEN + 8 * i + 4);
        for (uint32_t r = st; r <= en; r++) {
            uint32_t idx = (uint32_t)(r - cur_mst);
            if (BIT_GET(idx)) {
                S.dup_records++;
            } else {
                memcpy(cur_buf + (size_t)idx * rs, payload, rs);
                BIT_SET(idx);
                cur_count++;
            }
            payload += rs;
        }
    }
    state = R_RECEIVING;
}

static void handle_is_over(const uint8_t *b, ssize_t n)
{
    if (n < 17) return;
    uint32_t blast = get32(b + 1), round = get32(b + 13);

    if (blast < cur_blast) {                 /* already complete: our empty
                                                REC_MISS was lost, repeat it */
        send_rec_miss(blast, round, NULL, 0);
        return;
    }
    if (state == R_LINGERING || state == R_IDLE || blast > cur_blast) return;

    range_t rg[MAX_MISS_RANGES];
    int nr = 0;
    uint32_t i = 0;
    while (i < cur_nrec) {
        if (BIT_GET(i)) { i++; continue; }
        uint32_t j = i;
        while (j + 1 < cur_nrec && !BIT_GET(j + 1)) j++;
        if (nr == MAX_MISS_RANGES) { S.miss_truncated++; break; }
        rg[nr].st = (uint32_t)cur_mst + i;
        rg[nr].end = (uint32_t)cur_mst + j;
        nr++;
        i = j + 1;
    }
    send_rec_miss(blast, round, rg, nr);     /* send BEFORE advancing: the next
                                                begin_blast() may block on disk */
    if (nr == 0) {
        VLOG("receiver: blast %u complete\n", blast);
        complete_blast();
    }
}

static void write_csv(const char *path, const char *header, const char *row)
{
    FILE *f = fopen(path, "a");
    if (!f) { perror("csv"); return; }
    fseek(f, 0, SEEK_END);
    if (ftell(f) == 0) fprintf(f, "%s\n", header);
    fprintf(f, "%s\n", row);
    fclose(f);
}

static void usage(const char *p)
{
    fprintf(stderr,
        "usage: %s -p PORT -d OUTDIR [options]\n"
        "  -L MS     linger time after FIN / last blast in ms (default 2000)\n"
        "  -I SEC    abort if nothing is heard for SEC seconds (default 30)\n"
        "  -B NUM    blast buffers in the pool (default 3, max %d)\n"
        "  -D US     simulate slow disk: delay US microseconds per blast write\n"
        "  -b BYTES  socket receive buffer size (default 8388608)\n"
        "  -l P -u P -q P -x T:K:C -s SEED -G FILE   garbler options (see sender)\n"
        "  -o CSV    append stats row to CSV      -T TAG  tag column for CSV\n"
        "  -v        verbose\n", p, NBUF_MAX);
}

int main(int argc, char **argv)
{
    int port = 0;
    const char *outdir = ".", *csv = NULL, *tag = "run";
    double linger = 2.0, idle_s = 30.0, loss = 0;
    uint64_t seed = 1;
    int rcvbuf = 8 * 1024 * 1024;

    garbler_init("receiver");

    int opt;
    while ((opt = getopt(argc, argv, "p:d:L:I:B:D:b:l:u:q:x:s:G:o:T:v")) != -1) {
        switch (opt) {
        case 'p': port = atoi(optarg); break;
        case 'd': outdir = optarg; break;
        case 'L': linger = atof(optarg) / 1000.0; break;
        case 'I': idle_s = atof(optarg); break;
        case 'B': nbuf = atoi(optarg); break;
        case 'D': disk_delay_us = (unsigned)atoi(optarg); break;
        case 'b': rcvbuf = atoi(optarg); break;
        case 'l': loss = atof(optarg); garbler_set_loss(loss); break;
        case 'u': garbler_set_dup(atof(optarg)); break;
        case 'q': garbler_set_reorder(atof(optarg)); break;
        case 'x':
            if (garbler_add_schedule(optarg) < 0) {
                fprintf(stderr, "bad -x spec '%s'\n", optarg);
                return 1;
            }
            break;
        case 's': seed = strtoull(optarg, NULL, 10); break;
        case 'G': garbler_set_log(optarg); break;
        case 'o': csv = optarg; break;
        case 'T': tag = optarg; break;
        case 'v': verbose = 1; break;
        default: usage(argv[0]); return 1;
        }
    }
    garbler_set_seed(seed);
    if (port <= 0) { usage(argv[0]); return 1; }
    if (nbuf < 2 || nbuf > NBUF_MAX) { fprintf(stderr, "-B must be 2..%d\n", NBUF_MAX); return 1; }

    mkdir(outdir, 0755);

    sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) { perror("socket"); return 1; }
    int one = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    if (setsockopt(sock, SOL_SOCKET, SO_RCVBUFFORCE, &rcvbuf, sizeof rcvbuf) < 0)
        setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof rcvbuf);

    struct sockaddr_in me;
    memset(&me, 0, sizeof me);
    me.sin_family = AF_INET;
    me.sin_addr.s_addr = htonl(INADDR_ANY);
    me.sin_port = htons((uint16_t)port);
    if (bind(sock, (struct sockaddr *)&me, sizeof me) < 0) { perror("bind"); return 1; }

    pthread_t wt;
    int writer_started = 0;
    double t_start = 0, t_done = 0;
    double last_rx = now_s(), last_fin = 0;
    int got_fin = 0, exit_code = 3;
    uint8_t rx[PKT_BUF_SIZE];

    for (;;) {
        double now = now_s();
        if (state == R_LINGERING) {
            if ((got_fin && now - last_rx > linger) ||
                (!got_fin && now - last_rx > linger * 5)) { exit_code = 0; break; }
        } else if (now - last_rx > idle_s) {
            fprintf(stderr, "receiver: idle for %.0f s, aborting (state=%d)\n", idle_s, state);
            break;
        }

        struct pollfd pfd = { .fd = sock, .events = POLLIN };
        int pr = poll(&pfd, 1, 100);
        if (pr <= 0) continue;

        struct sockaddr_in from;
        socklen_t fl = sizeof from;
        ssize_t n = recvfrom(sock, rx, sizeof rx, 0, (struct sockaddr *)&from, &fl);
        if (n <= 0) continue;
        if (have_peer && (from.sin_port != peer.sin_port ||
                          from.sin_addr.s_addr != peer.sin_addr.s_addr))
            continue;                          /* only talk to our sender */
        last_rx = now_s();
        S.pkts_recv++;

        switch (rx[0]) {
        case PKT_FILE_HDR: {
            int was = have_peer;
            handle_file_hdr(rx, n, &from, outdir);
            if (!was && have_peer) {
                t_start = now_s();
                pthread_create(&wt, NULL, writer_thread, NULL);
                writer_started = 1;
                if (state == R_LINGERING) t_done = now_s();
            }
            break;
        }
        case PKT_DATA:
            if (have_peer) handle_data(rx, n);
            break;
        case PKT_IS_BLAST_OVER:
            if (have_peer) {
                enum rstate before = state;
                handle_is_over(rx, n);
                if (before != R_LINGERING && state == R_LINGERING) t_done = now_s();
            }
            break;
        case PKT_FIN:
            if (have_peer && state == R_LINGERING) {
                uint8_t fa = PKT_FIN_ACK;
                tx(&fa, 1);
                got_fin = 1;
                last_fin = now_s();
            }
            break;
        default:
            break;
        }
    }
    (void)last_fin;

    if (writer_started) {
        pthread_mutex_lock(&mu);
        shutting = 1;
        pthread_cond_broadcast(&cv_job);
        pthread_mutex_unlock(&mu);
        pthread_join(wt, NULL);
    }
    if (out_fd >= 0) {
        fsync(out_fd);
        close(out_fd);
    }
    garbler_flush();
    if (write_err) exit_code = 4;

    double elapsed = (t_done > t_start) ? t_done - t_start : 0;
    uint64_t sent = garbler_total_sent(), dropped = garbler_total_dropped();
    printf("BLAST receiver %s: %llu bytes expected, %u blast(s)\n",
           exit_code == 0 ? "done" : "ABORTED", ULL(file_size), nblasts);
    printf("  packets received   : %llu (data: %llu, stale/ignored: %llu, duplicate records: %llu)\n",
           ULL(S.pkts_recv), ULL(S.data_pkts), ULL(S.stale_pkts), ULL(S.dup_records));
    printf("  REC_MISS sent      : %llu (truncated lists: %llu)\n",
           ULL(S.miss_sent), ULL(S.miss_truncated));
    printf("  packets sent       : %llu (dropped by garbler: %llu)\n", ULL(sent), ULL(dropped));
    printf("  buffer-pool waits  : %llu (network thread blocked on disk)\n", ULL(buf_waits));

    if (csv) {
        char row[512];
        snprintf(row, sizeof row, "%s,%.4f,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%.4f",
                 tag, loss, ULL(seed), ULL(file_size), ULL(S.pkts_recv), ULL(S.data_pkts),
                 ULL(S.dup_records), ULL(S.stale_pkts), ULL(S.miss_sent),
                 ULL(S.miss_truncated), ULL(buf_waits), ULL(sent), elapsed);
        /* append dropped as the last column */
        char row2[600];
        snprintf(row2, sizeof row2, "%s,%llu", row, ULL(dropped));
        write_csv(csv,
                  "tag,loss,seed,file_bytes,pkts_recv,data_pkts,dup_records,"
                  "stale_pkts,miss_sent,miss_truncated,buf_waits,pkts_sent,"
                  "elapsed_s,pkts_dropped", row2);
    }
    return exit_code;
}

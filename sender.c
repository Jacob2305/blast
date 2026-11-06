/*
 * sender.c - BLAST file sender.
 *
 * Threads:
 *   reader thread  : reads the file record by record into a ring buffer
 *   network thread : (main) pulls records from the ring, builds DATA packets
 *                    (up to 16 records each), sends blasts, runs the
 *                    IS_BLAST_OVER / REC_MISS recovery loop, then FIN.
 *
 * Sender state machine (per blast, driven from main()):
 *   SETUP          send FILE_HDR, wait FILE_HDR_ACK     (timeout -> resend)
 *   BLASTING       stream the blast's records as DATA packets
 *   AWAIT_REC_MISS send IS_BLAST_OVER(round), wait REC_MISS (timeout -> resend)
 *   RESENDING      non-empty REC_MISS -> resend only those records, go back to
 *                  AWAIT_REC_MISS; empty REC_MISS -> next blast (or FIN)
 *   DISCONNECT     send FIN, wait FIN_ACK (bounded retries)
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <netdb.h>
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

#define RING_SLOTS 4096

static int verbose;
#define VLOG(...) do { if (verbose) fprintf(stderr, __VA_ARGS__); } while (0)

/* ------------------------------------------------------------------ */
/* ring buffer between disk-reader thread and network thread           */
/* ------------------------------------------------------------------ */
typedef struct {
    uint8_t *data;
    size_t slots;
    uint64_t head, tail;           /* produced / consumed record counts */
    int eof, err;
    pthread_mutex_t mu;
    pthread_cond_t not_empty, not_full;
} ring_t;

static ring_t ring;
static uint64_t ring_stalls;       /* times the network thread waited on disk */

static int in_fd;
static uint64_t file_size, total_records;
static uint32_t rs = 1024, M = 1000, N = 8;

static void *reader_thread(void *arg)
{
    (void)arg;
    for (uint64_t r = 0; r < total_records; r++) {
        pthread_mutex_lock(&ring.mu);
        while (ring.head - ring.tail == ring.slots)
            pthread_cond_wait(&ring.not_full, &ring.mu);
        uint8_t *slot = ring.data + (size_t)(ring.head % ring.slots) * rs;
        pthread_mutex_unlock(&ring.mu);

        /* The slot is exclusively ours until head++ below. */
        size_t got = 0;
        int err = 0;
        while (got < rs) {
            ssize_t n = pread(in_fd, slot + got, rs - got,
                              (off_t)(r * rs + got));
            if (n < 0) {
                if (errno == EINTR) continue;
                perror("pread");
                err = 1;
                break;
            }
            if (n == 0) break;
            got += (size_t)n;
        }
        if (!err && got < rs) memset(slot + got, 0, rs - got); /* pad last rec */

        pthread_mutex_lock(&ring.mu);
        if (err) {
            ring.err = 1;
            break;
        }
        ring.head++;
        pthread_cond_signal(&ring.not_empty);
        pthread_mutex_unlock(&ring.mu);
    }
    if (!ring.err) pthread_mutex_lock(&ring.mu);
    ring.eof = 1;
    pthread_cond_broadcast(&ring.not_empty);
    pthread_mutex_unlock(&ring.mu);
    return NULL;
}

/* Copy next record into dst. block=1 waits for the reader; block=0 returns 0
 * immediately if none is ready. Returns 1 on success, 0 otherwise. */
static int ring_get(uint8_t *dst, int block)
{
    pthread_mutex_lock(&ring.mu);
    int waited = 0;
    while (ring.head == ring.tail) {
        if (ring.eof || ring.err || !block) {
            pthread_mutex_unlock(&ring.mu);
            return 0;
        }
        if (!waited) { ring_stalls++; waited = 1; }
        pthread_cond_wait(&ring.not_empty, &ring.mu);
    }
    memcpy(dst, ring.data + (size_t)(ring.tail % ring.slots) * rs, rs);
    ring.tail++;
    pthread_cond_signal(&ring.not_full);
    pthread_mutex_unlock(&ring.mu);
    return 1;
}

/* ------------------------------------------------------------------ */
/* network helpers                                                      */
/* ------------------------------------------------------------------ */
static int sock;
static struct sockaddr_in peer;
static double rto_base = 0.05, rto_max = 2.0;
static int max_retries = 40;
static double gap_us;

static struct {
    uint64_t data_pkts, retx_pkts, retx_records, retx_rounds;
    uint64_t is_over_sent, timeouts, hdr_sent, fin_sent;
} S;

static uint8_t *bbuf;              /* current blast buffer: M * rs bytes */
static uint8_t txbuf[PKT_BUF_SIZE];

static void tx(const void *b, size_t n)
{
    g_sendto(sock, b, n, (struct sockaddr *)&peer, sizeof peer);
}

static int same_peer(const struct sockaddr_in *a)
{
    return a->sin_family == peer.sin_family &&
           a->sin_port == peer.sin_port &&
           a->sin_addr.s_addr == peer.sin_addr.s_addr;
}

/* Wait until `deadline` for a datagram from the peer. Returns its length, or
 * -1 on timeout. */
static ssize_t wait_pkt(uint8_t *buf, double deadline)
{
    for (;;) {
        double left = deadline - now_s();
        if (left <= 0) return -1;
        struct pollfd pfd = { .fd = sock, .events = POLLIN };
        int pr = poll(&pfd, 1, (int)(left * 1000.0) + 1);
        if (pr < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (pr == 0) return -1;
        struct sockaddr_in from;
        socklen_t fl = sizeof from;
        ssize_t n = recvfrom(sock, buf, PKT_BUF_SIZE, 0,
                             (struct sockaddr *)&from, &fl);
        if (n <= 0) continue;
        if (!same_peer(&from)) continue;
        return n;
    }
}

static double backoff(double rto)
{
    rto *= 1.5;
    return rto > rto_max ? rto_max : rto;
}

/* ---- phase 1: connection setup ---- */
static int do_setup(const char *remote_name)
{
    uint8_t pkt[512], rx[PKT_BUF_SIZE];
    size_t nl = strlen(remote_name);
    pkt[0] = PKT_FILE_HDR;
    put64(pkt + 1, file_size);
    put32(pkt + 9, rs);
    put32(pkt + 13, M);
    put32(pkt + 17, N);
    put16(pkt + 21, (uint16_t)nl);
    memcpy(pkt + 23, remote_name, nl);
    size_t len = 23 + nl;

    double rto = rto_base;
    int tries = 0;
    for (;;) {
        tx(pkt, len);
        S.hdr_sent++;
        double deadline = now_s() + rto;
        for (;;) {
            ssize_t n = wait_pkt(rx, deadline);
            if (n < 0) break;
            if (rx[0] == PKT_FILE_HDR_ACK && n >= 14) {
                if (rx[1] != 0) {
                    fprintf(stderr, "sender: receiver rejected FILE_HDR\n");
                    return -1;
                }
                if (get32(rx + 2) != rs) {
                    fprintf(stderr, "sender: receiver changed record size\n");
                    return -1;
                }
                uint32_t m2 = get32(rx + 6), n2 = get32(rx + 10);
                if (m2 < 1 || m2 > M) m2 = M;       /* receiver may only lower M */
                if (n2 < 1 || n2 > N) n2 = N;
                M = m2;
                N = n2;
                return 0;
            }
        }
        if (++tries > max_retries) {
            fprintf(stderr, "sender: no FILE_HDR_ACK after %d tries\n", tries);
            return -1;
        }
        S.timeouts++;
        rto = backoff(rto);
        VLOG("sender: FILE_HDR timeout, retry %d (rto=%.3f)\n", tries, rto);
    }
}

/* ---- phase 2: building and sending DATA packets ---- */
static void emit_data(uint32_t blast, uint32_t mst, uint32_t mfin,
                      const range_t *segs, int ns, int nrec, int is_retx)
{
    uint8_t *p = txbuf;
    p[0] = PKT_DATA;
    put32(p + 1, blast);
    put32(p + 5, mst);
    put32(p + 9, mfin);
    put16(p + 13, (uint16_t)ns);
    put16(p + 15, (uint16_t)nrec);
    size_t off = DATA_HDR_LEN;
    for (int i = 0; i < ns; i++) {
        put32(txbuf + off, segs[i].st);
        put32(txbuf + off + 4, segs[i].end);
        off += 8;
    }
    for (int i = 0; i < ns; i++) {
        for (uint32_t r = segs[i].st; r <= segs[i].end; r++) {
            memcpy(txbuf + off, bbuf + (size_t)(r - mst) * rs, rs);
            off += rs;
        }
    }
    tx(txbuf, off);
    S.data_pkts++;
    if (is_retx) {
        S.retx_pkts++;
        S.retx_records += (uint64_t)nrec;
    }
    if (gap_us > 0) usleep((useconds_t)gap_us);
}

/* Pack the given record ranges into packets of <=16 records and <=N segments
 * each, and send them. Records must already be in bbuf. */
static void send_ranges(const range_t *r, int nr, uint32_t blast,
                        uint32_t mst, uint32_t mfin, int is_retx)
{
    int i = 0;
    uint32_t pos = nr ? r[0].st : 0;
    while (i < nr) {
        range_t segs[MAX_SEGS_PER_PKT];
        int ns = 0, nrec = 0;
        while (i < nr && nrec < MAX_REC_PER_PKT && ns < (int)N) {
            uint32_t avail = r[i].end - pos + 1;
            uint32_t room = (uint32_t)(MAX_REC_PER_PKT - nrec);
            uint32_t take = avail < room ? avail : room;
            segs[ns].st = pos;
            segs[ns].end = pos + take - 1;
            ns++;
            nrec += (int)take;
            pos += take;
            if (pos > r[i].end) {
                i++;
                if (i < nr) pos = r[i].st;
            }
        }
        emit_data(blast, mst, mfin, segs, ns, nrec, is_retx);
    }
}

/* First transmission of blast b: pull records from the ring as the reader
 * produces them; packets hold however many records are ready (1..16). */
static int send_blast_first(uint32_t b, uint32_t mst, uint32_t cnt)
{
    uint32_t sent = 0;
    while (sent < cnt) {
        uint32_t want = cnt - sent;
        if (want > MAX_REC_PER_PKT) want = MAX_REC_PER_PKT;
        uint32_t got = 0;
        if (!ring_get(bbuf + (size_t)(sent + got) * rs, 1)) return -1;
        got = 1;
        while (got < want && ring_get(bbuf + (size_t)(sent + got) * rs, 0))
            got++;
        range_t r = { mst + sent, mst + sent + got - 1 };
        send_ranges(&r, 1, b, mst, mst + cnt - 1, 0);
        sent += got;
    }
    return 0;
}

/* Wait for a REC_MISS for (blast, round). Empty lists are accepted for any
 * round (the received set only grows, so "nothing missing" is always true).
 * Returns 1 on success, 0 on timeout. */
static int await_rec_miss(uint32_t blast, uint32_t round, uint32_t mst,
                          uint32_t mfin, range_t *out, int *nout,
                          double deadline)
{
    uint8_t rx[PKT_BUF_SIZE];
    for (;;) {
        ssize_t n = wait_pkt(rx, deadline);
        if (n < 0) return 0;
        if (rx[0] != PKT_REC_MISS || n < 11) continue;
        uint32_t b = get32(rx + 1), rd = get32(rx + 5);
        uint16_t nr = get16(rx + 9);
        if (b != blast) continue;
        if (nr > MAX_MISS_RANGES || n < 11 + 8 * (ssize_t)nr) continue;
        if (nr != 0 && rd != round) continue;      /* stale non-empty reply */
        int ok = 1;
        for (int i = 0; i < nr; i++) {
            uint32_t st = get32(rx + 11 + 8 * i), en = get32(rx + 15 + 8 * i);
            if (st > en || st < mst || en > mfin) { ok = 0; break; }
            out[i].st = st;
            out[i].end = en;
        }
        if (!ok) continue;
        *nout = nr;
        return 1;
    }
}

/* ---- stats output ---- */
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
        "usage: %s -a HOST -p PORT -f FILE [options]\n"
        "  -n NAME   file name to use at the receiver (default: basename of FILE)\n"
        "  -r SIZE   record size 256|512|1024 (default 1024)\n"
        "  -M NUM    records per blast (default 1000)\n"
        "  -N NUM    max segments per packet, 1..16 (default 8)\n"
        "  -t MS     base retransmission timeout in ms (default 50)\n"
        "  -R NUM    max consecutive timeouts before giving up (default 40)\n"
        "  -g US     pause between DATA packets in microseconds (default 0)\n"
        "  -l P      garbler: random loss probability 0..1\n"
        "  -u P      garbler: duplicate probability\n"
        "  -q P      garbler: reorder probability (DATA only)\n"
        "  -x T:K:C  garbler: drop C packets of type T starting at the Kth\n"
        "  -s SEED   garbler RNG seed (default 1)\n"
        "  -G FILE   garbler drop log\n"
        "  -o CSV    append stats row to CSV      -T TAG  tag column for CSV\n"
        "  -v        verbose\n", p);
}

int main(int argc, char **argv)
{
    const char *host = NULL, *path = NULL, *rname = NULL, *csv = NULL;
    const char *tag = "run";
    int port = 0;
    double loss = 0;
    uint64_t seed = 1;

    garbler_init("sender");

    int opt;
    while ((opt = getopt(argc, argv, "a:p:f:n:r:M:N:l:s:t:R:g:o:T:x:u:q:G:v")) != -1) {
        switch (opt) {
        case 'a': host = optarg; break;
        case 'p': port = atoi(optarg); break;
        case 'f': path = optarg; break;
        case 'n': rname = optarg; break;
        case 'r': rs = (uint32_t)atoi(optarg); break;
        case 'M': M = (uint32_t)atoi(optarg); break;
        case 'N': N = (uint32_t)atoi(optarg); break;
        case 'l': loss = atof(optarg); garbler_set_loss(loss); break;
        case 's': seed = strtoull(optarg, NULL, 10); break;
        case 't': rto_base = atof(optarg) / 1000.0; break;
        case 'R': max_retries = atoi(optarg); break;
        case 'g': gap_us = atof(optarg); break;
        case 'o': csv = optarg; break;
        case 'T': tag = optarg; break;
        case 'x':
            if (garbler_add_schedule(optarg) < 0) {
                fprintf(stderr, "bad -x spec '%s'\n", optarg);
                return 1;
            }
            break;
        case 'u': garbler_set_dup(atof(optarg)); break;
        case 'q': garbler_set_reorder(atof(optarg)); break;
        case 'G': garbler_set_log(optarg); break;
        case 'v': verbose = 1; break;
        default: usage(argv[0]); return 1;
        }
    }
    garbler_set_seed(seed);

    if (!host || !path || port <= 0) { usage(argv[0]); return 1; }
    if (!valid_record_size(rs)) { fprintf(stderr, "record size must be 256/512/1024\n"); return 1; }
    if (M < 1 || M > MAX_M) { fprintf(stderr, "M must be 1..%d\n", MAX_M); return 1; }
    if (N < 1 || N > MAX_SEGS_PER_PKT) { fprintf(stderr, "N must be 1..%d\n", MAX_SEGS_PER_PKT); return 1; }

    if (!rname) {
        const char *s = strrchr(path, '/');
        rname = s ? s + 1 : path;
    }
    if (strlen(rname) == 0 || strlen(rname) > MAX_FNAME) {
        fprintf(stderr, "bad remote file name\n");
        return 1;
    }

    in_fd = open(path, O_RDONLY);
    if (in_fd < 0) { perror("open input"); return 1; }
    struct stat st;
    if (fstat(in_fd, &st) < 0) { perror("fstat"); return 1; }
    file_size = (uint64_t)st.st_size;
    total_records = (file_size + rs - 1) / rs;

    struct addrinfo hints, *res;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    char portstr[16];
    snprintf(portstr, sizeof portstr, "%d", port);
    if (getaddrinfo(host, portstr, &hints, &res) != 0) {
        fprintf(stderr, "cannot resolve %s\n", host);
        return 1;
    }
    memcpy(&peer, res->ai_addr, sizeof peer);
    freeaddrinfo(res);

    sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) { perror("socket"); return 1; }
    int bufsz = 4 * 1024 * 1024;
    setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &bufsz, sizeof bufsz);
    setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &bufsz, sizeof bufsz);

    double t0 = now_s();

    /* ---- Phase 1 ---- */
    if (do_setup(rname) < 0) { garbler_flush(); return 2; }
    uint32_t nblasts = (uint32_t)((total_records + M - 1) / M);
    VLOG("sender: setup ok: file=%llu bytes, %llu records, rs=%u M=%u N=%u, %u blasts\n",
         ULL(file_size), ULL(total_records), rs, M, N, nblasts);

    ring.slots = RING_SLOTS;
    ring.data = malloc((size_t)RING_SLOTS * rs);
    bbuf = malloc((size_t)M * rs);
    if (!ring.data || !bbuf) { fprintf(stderr, "out of memory\n"); return 1; }
    pthread_mutex_init(&ring.mu, NULL);
    pthread_cond_init(&ring.not_empty, NULL);
    pthread_cond_init(&ring.not_full, NULL);
    pthread_t rt;
    pthread_create(&rt, NULL, reader_thread, NULL);

    /* ---- Phase 2 ---- */
    static range_t miss[MAX_MISS_RANGES];
    for (uint32_t b = 0; b < nblasts; b++) {
        uint32_t mst = b * M;
        uint32_t cnt = (uint32_t)((total_records - mst) < M ? (total_records - mst) : M);
        uint32_t mfin = mst + cnt - 1;

        if (send_blast_first(b, mst, cnt) < 0) {
            fprintf(stderr, "sender: disk read failed\n");
            garbler_flush();
            return 2;
        }

        uint32_t round = 0;
        int consec = 0;
        double rto = rto_base;
        for (;;) {
            round++;
            uint8_t q[17];
            q[0] = PKT_IS_BLAST_OVER;
            put32(q + 1, b);
            put32(q + 5, mst);
            put32(q + 9, mfin);
            put32(q + 13, round);
            tx(q, sizeof q);
            S.is_over_sent++;

            int nm = 0;
            int got = await_rec_miss(b, round, mst, mfin, miss, &nm, now_s() + rto);
            if (!got) {
                S.timeouts++;
                if (++consec > max_retries) {
                    fprintf(stderr, "sender: blast %u: no REC_MISS after %d tries, giving up\n", b, consec);
                    garbler_flush();
                    return 2;
                }
                rto = backoff(rto);
                VLOG("sender: blast %u round %u timeout\n", b, round);
                continue;
            }
            consec = 0;
            rto = rto_base;
            if (nm == 0) break;                     /* blast complete */

            S.retx_rounds++;
            VLOG("sender: blast %u round %u: %d missing range(s)\n", b, round, nm);
            send_ranges(miss, nm, b, mst, mfin, 1);
        }
    }
    double t_xfer = now_s() - t0;

    /* ---- Phase 3: disconnect ---- */
    {
        uint8_t fin = PKT_FIN, rx[PKT_BUF_SIZE];
        double rto = rto_base;
        int acked = 0;
        for (int tries = 0; tries < 6 && !acked; tries++) {
            tx(&fin, 1);
            S.fin_sent++;
            double deadline = now_s() + rto;
            for (;;) {
                ssize_t n = wait_pkt(rx, deadline);
                if (n < 0) break;
                if (rx[0] == PKT_FIN_ACK) { acked = 1; break; }
            }
            rto = backoff(rto);
        }
        if (!acked) fprintf(stderr, "sender: warning: FIN_ACK never received (data was confirmed)\n");
    }
    double t_end = now_s() - t0;
    garbler_flush();
    pthread_join(rt, NULL);

    /* ---- statistics ---- */
    double mb = (double)file_size / 1e6;
    double thr = t_end > 0 ? mb / t_end : 0;
    double thr_x = t_xfer > 0 ? mb / t_xfer : 0;
    uint64_t sent = garbler_total_sent(), dropped = garbler_total_dropped();

    printf("BLAST sender done: %llu bytes, rs=%u M=%u N=%u\n", ULL(file_size), rs, M, N);
    printf("  elapsed            : %.3f s (transfer only: %.3f s)\n", t_end, t_xfer);
    printf("  throughput         : %.2f MB/s (transfer only: %.2f MB/s)\n", thr, thr_x);
    printf("  packets sent       : %llu (dropped by garbler: %llu)\n", ULL(sent), ULL(dropped));
    printf("  data packets       : %llu (retransmitted: %llu, %llu records)\n",
           ULL(S.data_pkts), ULL(S.retx_pkts), ULL(S.retx_records));
    printf("  retransmit rounds  : %llu   IS_BLAST_OVER sent: %llu   timeouts: %llu\n",
           ULL(S.retx_rounds), ULL(S.is_over_sent), ULL(S.timeouts));
    printf("  disk stalls (ring) : %llu\n", ULL(ring_stalls));

    if (csv) {
        char row[1024];
        snprintf(row, sizeof row,
                 "%s,%.4f,%llu,%llu,%u,%u,%u,%.4f,%.4f,%.4f,%.4f,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu",
                 tag, loss, ULL(seed), ULL(file_size), rs, M, N, t_end, t_xfer, thr, thr_x,
                 ULL(sent), ULL(dropped), ULL(S.data_pkts), ULL(S.retx_pkts),
                 ULL(S.retx_records), ULL(S.retx_rounds), ULL(S.is_over_sent),
                 ULL(S.timeouts), ULL(ring_stalls));
        write_csv(csv,
                  "tag,loss,seed,file_bytes,record_size,M,N,elapsed_s,xfer_s,"
                  "throughput_MBps,xfer_throughput_MBps,pkts_sent,pkts_dropped,"
                  "data_pkts,retx_pkts,retx_records,retx_rounds,is_over_sent,"
                  "timeouts,ring_stalls", row);
    }
    return 0;
}

#include "garbler.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>

#define MAX_SCHED 16

typedef struct { int type; uint64_t first, count; } sched_t;

static double p_loss, p_dup, p_reorder;
static uint64_t rng_state = 88172645463325252ULL;
static garbler_stats_t stats;
static uint64_t seen[PKT_TYPE_MAX];
static sched_t sched[MAX_SCHED];
static int nsched;
static FILE *logf;
static char role[16] = "node";

static struct {
    int valid;
    int fd;
    size_t len;
    socklen_t tolen;
    struct sockaddr_storage to;
    uint8_t buf[PKT_BUF_SIZE];
} held;

/* xorshift64* ; seeded through splitmix64 so nearby seeds diverge quickly */
static uint64_t rng_next(void)
{
    uint64_t x = rng_state;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    rng_state = x;
    return x * 2685821657736338717ULL;
}

static double rng_unif(void)
{
    return (double)(rng_next() >> 11) * (1.0 / 9007199254740992.0);
}

void garbler_init(const char *r)
{
    strncpy(role, r, sizeof role - 1);
    role[sizeof role - 1] = '\0';
}

void garbler_set_loss(double p)    { p_loss = p; }
void garbler_set_dup(double p)     { p_dup = p; }
void garbler_set_reorder(double p) { p_reorder = p; }

void garbler_set_seed(uint64_t seed)
{
    uint64_t z = seed + 0x9E3779B97F4A7C15ULL;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    z ^= z >> 31;
    rng_state = z ? z : 1;
}

int garbler_add_schedule(const char *spec)
{
    int type;
    unsigned long long first, count = 1;
    int n = sscanf(spec, "%d:%llu:%llu", &type, &first, &count);
    if (n < 2) return -1;
    if (type <= 0 || type >= PKT_TYPE_MAX || first < 1 || count < 1) return -1;
    if (nsched >= MAX_SCHED) return -1;
    sched[nsched].type = type;
    sched[nsched].first = first;
    sched[nsched].count = count;
    nsched++;
    return 0;
}

void garbler_set_log(const char *path)
{
    logf = fopen(path, "a");
    if (!logf) perror("garbler log");
}

static void log_event(const char *what, const uint8_t *buf, size_t len)
{
    if (!logf || len < 5) return;
    unsigned t = buf[0];
    if (t == PKT_DATA || t == PKT_IS_BLAST_OVER || t == PKT_REC_MISS)
        fprintf(logf, "%s %s type=%u blast=%u len=%zu\n", role, what, t,
                get32(buf + 1), len);
    else
        fprintf(logf, "%s %s type=%u len=%zu\n", role, what, t, len);
    fflush(logf);
}

/* One protocol-level send: apply drop rules, then put it on the wire. */
static void do_send(int fd, const uint8_t *buf, size_t len,
                    const struct sockaddr *to, socklen_t tolen)
{
    int type = buf[0];
    if (type <= 0 || type >= PKT_TYPE_MAX) type = 0;

    stats.sent[type]++;
    seen[type]++;

    int drop = 0;
    for (int i = 0; i < nsched; i++) {
        if (sched[i].type == type && seen[type] >= sched[i].first &&
            seen[type] < sched[i].first + sched[i].count)
            drop = 1;
    }
    if (!drop && p_loss > 0 && rng_unif() < p_loss) drop = 1;

    if (drop) {
        stats.dropped[type]++;
        log_event("DROP", buf, len);
        return;
    }

    sendto(fd, buf, len, 0, to, tolen);
    if (p_dup > 0 && rng_unif() < p_dup) {
        stats.duplicated++;
        log_event("DUP", buf, len);
        sendto(fd, buf, len, 0, to, tolen);
    }
}

ssize_t g_sendto(int fd, const void *buf, size_t len,
                 const struct sockaddr *to, socklen_t tolen)
{
    const uint8_t *b = (const uint8_t *)buf;
    if (len == 0 || len > PKT_BUF_SIZE) {
        errno = EMSGSIZE;
        return -1;
    }

    /* Reordering: hold back one DATA packet and release it after the next
     * send of any kind, so the two swap places on the wire. */
    if (b[0] == PKT_DATA && p_reorder > 0 && !held.valid &&
        rng_unif() < p_reorder) {
        held.valid = 1;
        held.fd = fd;
        held.len = len;
        held.tolen = tolen;
        memcpy(&held.to, to, tolen);
        memcpy(held.buf, b, len);
        stats.reordered++;
        log_event("HOLD", b, len);
        return (ssize_t)len;
    }

    do_send(fd, b, len, to, tolen);

    if (held.valid) {
        held.valid = 0;
        do_send(held.fd, held.buf, held.len,
                (struct sockaddr *)&held.to, held.tolen);
    }
    return (ssize_t)len;
}

void garbler_flush(void)
{
    if (held.valid) {
        held.valid = 0;
        do_send(held.fd, held.buf, held.len,
                (struct sockaddr *)&held.to, held.tolen);
    }
    if (logf) fflush(logf);
}

const garbler_stats_t *garbler_stats(void) { return &stats; }

uint64_t garbler_total_sent(void)
{
    uint64_t s = 0;
    for (int i = 0; i < PKT_TYPE_MAX; i++) s += stats.sent[i];
    return s;
}

uint64_t garbler_total_dropped(void)
{
    uint64_t s = 0;
    for (int i = 0; i < PKT_TYPE_MAX; i++) s += stats.dropped[i];
    return s;
}

/*
 * common.h - constants, wire-format helpers and small utilities shared by the
 *            BLAST sender, receiver and garbler.
 *
 * All multi-byte wire fields are big-endian. Record numbers are 0-based
 * internally (the spec's Fig. 2 uses 1-based numbers; it is only a labeling
 * difference).
 *
 * Wire formats (offsets in bytes):
 *
 *  FILE_HDR       [0]=1  [1..8]=file_size(u64) [9..12]=record_size [13..16]=M
 *                 [17..20]=N  [21..22]=name_len  [23..]=file name
 *  FILE_HDR_ACK   [0]=2  [1]=status(0 ok,1 reject) [2..5]=record_size
 *                 [6..9]=M accepted  [10..13]=N accepted
 *  DATA           [0]=3  [1..4]=blast_id [5..8]=Mst [9..12]=Mfin
 *                 [13..14]=nsegs [15..16]=nrec_total
 *                 then nsegs * (st_rec u32, end_rec u32), then nrec*record_size
 *                 payload bytes (segments in order, records in order)
 *  IS_BLAST_OVER  [0]=4  [1..4]=blast_id [5..8]=Mst [9..12]=Mfin [13..16]=round
 *  REC_MISS       [0]=5  [1..4]=blast_id [5..8]=round(echo) [9..10]=nranges
 *                 then nranges * (st_rec u32, end_rec u32)
 *  FIN            [0]=6
 *  FIN_ACK        [0]=7
 */
#ifndef BLAST_COMMON_H
#define BLAST_COMMON_H

#include <stdint.h>
#include <string.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>

enum pkt_type {
    PKT_FILE_HDR      = 1,
    PKT_FILE_HDR_ACK  = 2,
    PKT_DATA          = 3,
    PKT_IS_BLAST_OVER = 4,
    PKT_REC_MISS      = 5,
    PKT_FIN           = 6,
    PKT_FIN_ACK       = 7,
    PKT_TYPE_MAX      = 8
};

#define MAX_REC_PER_PKT   16     /* spec: at most 16 records per packet        */
#define MAX_SEGS_PER_PKT  16     /* N can never usefully exceed 16             */
#define MAX_RECORD_SIZE   1024
#define MAX_M             10000  /* spec: M in 200..10000 (we allow M >= 1 so  */
                                 /* tests can use tiny blasts)                 */
#define MAX_MISS_RANGES   128    /* bound on one REC_MISS (<= ~1 KB)           */
#define MAX_FNAME         255
#define PKT_BUF_SIZE      65536
#define DATA_HDR_LEN      17     /* type+blast+mst+mfin+nsegs+nrec             */

typedef struct { uint32_t st, end; } range_t;

static inline void put16(uint8_t *p, uint16_t v)
{
    p[0] = (uint8_t)(v >> 8);
    p[1] = (uint8_t)v;
}
static inline void put32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);  p[3] = (uint8_t)v;
}
static inline void put64(uint8_t *p, uint64_t v)
{
    put32(p, (uint32_t)(v >> 32));
    put32(p + 4, (uint32_t)v);
}
static inline uint16_t get16(const uint8_t *p)
{
    return (uint16_t)((p[0] << 8) | p[1]);
}
static inline uint32_t get32(const uint8_t *p)
{
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] << 8)  | (uint32_t)p[3];
}
static inline uint64_t get64(const uint8_t *p)
{
    return ((uint64_t)get32(p) << 32) | get32(p + 4);
}

static inline double now_s(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static inline int valid_record_size(uint32_t rs)
{
    return rs == 256 || rs == 512 || rs == 1024;
}

#define ULL(x) ((unsigned long long)(x))

#endif /* BLAST_COMMON_H */

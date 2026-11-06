/*
 * garbler.h - packet-loss injection layer ("garbler") for BLAST.
 *
 * Every outbound datagram goes through g_sendto(). The garbler can:
 *   - drop packets independently with probability p (Bernoulli loss)
 *   - drop packets deterministically ("drop the Kth..K+C-1th packet of type T")
 *   - duplicate packets with probability p_dup
 *   - reorder DATA packets with probability p_reorder (hold one packet and
 *     release it after the next send)
 * It is seeded, so a run can be reproduced. Both sender and receiver link it,
 * so loss applies in BOTH directions (DATA/FILE_HDR/IS_BLAST_OVER/FIN going
 * out, and FILE_HDR_ACK/REC_MISS/FIN_ACK coming back).
 *
 * Not thread-safe: call it only from the network thread (that is how
 * sender.c / receiver.c use it).
 */
#ifndef GARBLER_H
#define GARBLER_H

#include "common.h"
#include <stdio.h>

typedef struct {
    uint64_t sent[PKT_TYPE_MAX];     /* protocol-level sends, per type        */
    uint64_t dropped[PKT_TYPE_MAX];  /* of those, how many the garbler dropped */
    uint64_t duplicated;
    uint64_t reordered;
} garbler_stats_t;

void garbler_init(const char *role);          /* role name used in log lines */
void garbler_set_loss(double p);
void garbler_set_dup(double p);
void garbler_set_reorder(double p);
void garbler_set_seed(uint64_t seed);
int  garbler_add_schedule(const char *spec);  /* "type:first[:count]"        */
void garbler_set_log(const char *path);

ssize_t g_sendto(int fd, const void *buf, size_t len,
                 const struct sockaddr *to, socklen_t tolen);
void garbler_flush(void);                     /* release a held packet       */

const garbler_stats_t *garbler_stats(void);
uint64_t garbler_total_sent(void);
uint64_t garbler_total_dropped(void);

#endif /* GARBLER_H */

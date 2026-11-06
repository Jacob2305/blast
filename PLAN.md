# BLAST project plan (CSc412, due Nov 18, 2026)

Team: Mohammed Hossain + Jacob Li. Language: C (POSIX sockets, pthreads) on Linux.

## 1. What's in this folder

| File | What it is | Doc section it covers |
|---|---|---|
| `common.h` | packet types, wire format, constants | 3 Protocol Design |
| `garbler.h/.c` | loss/dup/reorder injection + deterministic drops, seeded, logs every drop | 2 Deliverables, 5 Risks |
| `sender.c` | disk-reader thread + ring buffer, network thread, sender state machine | 4, 7 (role A) |
| `receiver.c` | network thread, bitmap, REC_MISS, writer thread + buffer pool, linger | 4, 7 (role B) |
| `run_tests.sh` | 23 correctness cases (every item in Section 6 "Correctness") | 6 |
| `run_experiments.sh`, `plot.py` | throughput-vs-loss sweep, secondary sweep, graph | 6 "Experiments" |
| `Makefile` | `make`, `make test`, `make experiments` | |

## 2. Design decisions (these answer the blank/"decision needed" items in the doc)

**Packet layout.** All fields big-endian; full byte-level layout is in the header comment of `common.h`.
DATA = `type, blast_id, Mst, Mfin, nsegs, nrec`, then `nsegs` x `(st_rec, end_rec)`, then the record payloads.
Records are numbered from 0 internally (spec Fig. 2 counts from 1; labeling only).

**Parameter negotiation.** Sender proposes record size (256/512/1024), M, N in `FILE_HDR`. Receiver may *lower* M and N
(it owns the buffers) or reject (bad record size / cannot open file). Accepted values are fixed for the run.

**State machines.**
- Sender: SETUP -> BLASTING -> AWAIT_REC_MISS <-> RESENDING -> (next blast) -> DISCONNECT.
  Every wait has a timeout; a timeout resends the last control packet (`FILE_HDR`, `IS_BLAST_OVER`, `FIN`).
- Receiver: IDLE -> ACK_SENT -> RECEIVING -> LINGERING.

**Bounded REC_MISS.** One REC_MISS carries at most 128 `(start,end)` ranges (~1 KB). If more ranges are missing, the
list is truncated; the sender fixes those, asks again, and gets the rest next round. An *empty* list always means
"nothing missing", so truncation can never end a blast early.

**Recovery loop + cap.** `IS_BLAST_OVER(round)` / `REC_MISS(round echo)` repeats until the list is empty. Stale non-empty
replies (wrong round) are ignored. Timeout starts at `-t` ms (default 50) and grows x1.5 per consecutive timeout up to 2 s;
after `-R` consecutive timeouts (default 40) the sender gives up with exit code 2, so it always terminates.

**Linger.** After the last empty REC_MISS the receiver keeps answering: retransmitted `IS_BLAST_OVER` -> empty REC_MISS,
`FIN` -> `FIN_ACK`. It exits 2 s (`-L`) after the last packet once it has seen FIN, or 10 s after the last packet if FIN
never comes. The receiver also aborts if it hears nothing for 30 s (`-I`).

**MTU / fragmentation decision.** We *accept* IP fragmentation (a 16-record x 1024 B packet is ~16 KB, over the 1500 B
MTU). Capping packets at the MTU would force ~1 record per packet and kill throughput. Consequence: a single lost
fragment drops the whole packet, so effective loss on a real network is higher than the garbler rate. State this in the report.

**Writer thread flushes per completed blast** (not per contiguous run inside a blast). The receiver has a pool of blast
buffers (`-B`, default 3); the writer drains them while the network thread fills the next one. The network thread only
blocks if *every* buffer is waiting on the disk. Test with `-D <us>` (artificial disk delay).

**Garbler.** Runs on the send path of both programs, so loss hits both directions (DATA/FILE_HDR/IS_BLAST_OVER/FIN out,
FILE_HDR_ACK/REC_MISS/FIN_ACK back). Same rate for every packet type. Options: `-l` random loss, `-u` duplicates,
`-q` reordering (DATA only), `-x T:K:C` drop C packets of type T starting at the Kth, `-s` seed, `-G` drop log.

## 3. How to run

```bash
make                       # builds blast_sender and blast_receiver
make test                  # 23 correctness cases, ~20 s
./run_experiments.sh       # full sweep + graph (needs python3 + matplotlib for the PNG)

# manual transfer, two terminals (or two machines):
./blast_receiver -p 5000 -d ./out
./blast_sender -a <receiver-ip> -p 5000 -f bigfile.bin -n bigfile.bin -r 1024 -M 2000 -N 8 -l 0.1
```

Experiment knobs: `LOSSES="0 5 10" RUNS=3 RTO=10 SEC=0 ./run_experiments.sh outdir`.
Run it from inside this folder; results land in `results/` (`sender.csv`, `receiver.csv`, `summary_main.csv`, `throughput_vs_loss.png`).

## 4. Timeline (today is Nov 6, due Nov 18)

| Dates | Goal | Done when |
|---|---|---|
| Nov 6-7 | Read the code end to end, split ownership, both of you build + run `make test` on your own machines | each of you can explain your half |
| Nov 8-9 | Real two-machine run (two laptops/VMs/lab machines on one network), see whether kernel buffer drops appear at 0% garbler loss | 12 MB transfer works across a real link |
| Nov 10-12 | Final experiments: main sweep (5+ runs per loss rate), secondary sweep (record size x N), pick the RTO you will report | `results/` populated, graph looks sane |
| Nov 13-15 | Write the report: design, graph + explanation, hypothesis vs result, roles | full draft |
| Nov 16-17 | Proofread, re-run `make test` from a clean checkout, package source + report | everything builds from scratch |
| Nov 18 | Submit | |

## 5. Spec checklist

- [x] Sender + receiver, 3 phases (setup / blasts / disconnect with linger)
- [x] Variable records per packet (<=16), segment list in header, up to N segments
- [x] Timeouts on `FILE_HDR`, `IS_BLAST_OVER` (and `FIN`); retransmission-based recovery
- [x] Disk read / network / disk write decoupled (ring buffer + writer thread + buffer pool)
- [x] Garbler with configurable loss rate
- [x] Per-run stats: packets sent/lost, retransmissions, rounds, elapsed time, throughput (printed + CSV)
- [x] Throughput-vs-loss graph script
- [ ] **You:** run the real experiments on your hardware, write the report, state each person's role

## 6. Things to know before you run the experiments

1. **The retransmission timeout dominates results on loopback.** Loopback RTT is ~0.1 ms, so a 50 ms timeout is
   enormous. In our test at 10% loss, runs with 0 timeouts hit ~428 MB/s and runs with 4-5 timeouts hit ~41 MB/s (huge
   variance). At 30% loss, dropping `RTO` from 50 ms to 5 ms moved throughput from ~2.5 to ~30 MB/s. Pick the RTO
   deliberately (rule of thumb: a few x the real RTT), state it in the report, and use the same value for every run.
   This is also your best answer to "how does throughput degrade as loss increases, and why?"
2. **Kernel socket-buffer drops can look like extra loss.** The receiver asks for an 8 MB receive buffer, but a non-root
   Linux user is capped by `net.core.rmem_max` (often ~200 KB). If you see retransmissions at 0% garbler loss, either raise
   it (`sudo sysctl -w net.core.rmem_max=8388608`) or pace the sender with `-g 50` (50 us between packets; `GAP=50` in the
   experiment script).
3. **Warm-up / realism.** Our sandbox numbers (400+ MB/s) are loopback memory copies. Over a real link expect the link
   speed to be the ceiling. Use the two-machine run for the numbers you report.
4. **Hypothesis check.** Doc Section 7 predicts slow degradation then a sharp drop. The quick run agrees in shape (0% ->
   10% -> 30% falls steeply once timeouts start to dominate), but confirm with the full sweep before you claim it.
5. **Optional TCP comparison** (Section 7, second bullet) is *not* implemented; only do it if you have time left.

## 7. Division of work (matches doc Section 8)

- **A (sender):** `sender.c`: reader thread + ring buffer, packet construction, sender state machine + timeouts
- **B (receiver):** `receiver.c`: bitmap, REC_MISS generation, writer thread + pool, linger/disconnect
- **Shared:** `garbler.c`, statistics/CSV, `run_tests.sh`, `run_experiments.sh`, experiment runs, the report

Both of you should read *both* halves; the protocol only makes sense as a pair, and the grader will ask about it.

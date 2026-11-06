#!/usr/bin/env bash
# Correctness test suite for BLAST (Section 6 "Correctness" of the spec doc).
# Runs sender+receiver on localhost for each case and checks the received file
# is byte-identical (cmp). Garbler options inject the failures.
#
# usage: ./run_tests.sh            (all cases)
#        RTO=20 ./run_tests.sh     (change the sender's base timeout in ms)
cd "$(dirname "$0")" || exit 1
[ -x ./blast_sender ] && [ -x ./blast_receiver ] || make >/dev/null || exit 1

TMP=$(mktemp -d "${TMPDIR:-/tmp}/blast_tests.XXXXXX")
RTO=${RTO:-30}
PASS=0
FAIL=0

# run_case NAME SIZE_BYTES REC_SIZE M N "SENDER_ARGS" "RECEIVER_ARGS" [ok|fail]
#   ok   : sender must exit 0, receiver must exit 0, files must match
#   fail : sender must give up (nonzero, not a hang), receiver must exit on its own
run_case() {
    local name=$1 size=$2 rs=$3 M=$4 N=$5 sx=$6 rx=$7 expect=${8:-ok}
    local d="$TMP/$name"
    mkdir -p "$d/out"
    head -c "$size" /dev/urandom > "$d/in.bin"
    local port=$((20000 + RANDOM % 30000))
    local t0 t1 sc rc verdict why=""

    # shellcheck disable=SC2086
    timeout 120 ./blast_receiver -p "$port" -d "$d/out" -L 300 -I 20 $rx \
        > "$d/recv.log" 2>&1 &
    local rpid=$!
    sleep 0.2

    t0=$(date +%s.%N)
    # shellcheck disable=SC2086
    timeout 120 ./blast_sender -a 127.0.0.1 -p "$port" -f "$d/in.bin" -n out.bin \
        -r "$rs" -M "$M" -N "$N" -t "$RTO" $sx > "$d/send.log" 2>&1
    sc=$?
    wait "$rpid"
    rc=$?
    t1=$(date +%s.%N)

    verdict=PASS
    if [ "$expect" = ok ]; then
        [ $sc -eq 0 ] || { verdict=FAIL; why="sender rc=$sc"; }
        [ $rc -eq 0 ] || { verdict=FAIL; why="$why receiver rc=$rc"; }
        cmp -s "$d/in.bin" "$d/out/out.bin" || { verdict=FAIL; why="$why files differ"; }
    else
        { [ $sc -ne 0 ] && [ $sc -ne 124 ]; } || { verdict=FAIL; why="sender rc=$sc (expected clean give-up)"; }
        [ $rc -ne 124 ] || { verdict=FAIL; why="$why receiver hung"; }
    fi

    printf '%-4s %-34s %6.1fs  %s\n' "$verdict" "$name" "$(echo "$t1 - $t0" | bc)" "$why"
    if [ $verdict = PASS ]; then PASS=$((PASS + 1)); else FAIL=$((FAIL + 1)); echo "     logs: $d"; fi
}

echo "BLAST correctness tests (logs in $TMP)"
echo "----------------------------------------------------------------"
# --- file / blast geometry ---
run_case odd_size_not_multiple_of_rs   1000003 512  200 4  "" ""
run_case final_blast_smaller_than_M    446464  1024 200 4  "" ""      # 436 recs = 200+200+36
run_case single_blast_M_bigger_than_file 100000 1024 1000 8 "" ""
run_case one_byte_file                 1       256  200 2  "" ""
run_case empty_file                    0       512  200 2  "" ""
run_case rs_256_N_1                    500000  256  300 1  "" ""
run_case rs_1024_N_16                  800000  1024 500 16 "" ""

# --- lost control packets (deterministic drops) ---
run_case lost_FILE_HDR                 200000  1024 1000 8 "-x 1:1:2" ""
run_case lost_FILE_HDR_ACK             200000  1024 1000 8 "" "-x 2:1:2"
run_case lost_IS_BLAST_OVER            200000  1024 1000 8 "-x 4:1:2" ""
run_case lost_final_REC_MISS           100000  1024 1000 8 "" "-x 5:1:2"   # 1 blast: REC_MISS #1 is the final empty one
run_case lost_REC_MISS_multi_blast     446464  1024 100 8  "" "-x 5:2:3"
run_case lost_FIN                      100000  1024 1000 8 "-x 6:1:2" ""
run_case lost_FIN_ACK                  100000  1024 1000 8 "" "-x 7:1:3"
run_case lost_first_DATA_packets       100000  1024 1000 8 "-x 3:1:5" ""

# --- duplicates and reordering ---
run_case duplicate_packets_both_ways   1000003 512  200 4  "-u 0.3 -s 11" "-u 0.3 -s 12"
run_case reordered_DATA                1000003 512  200 4  "-q 0.3 -s 13" ""
run_case dup_and_reorder_and_loss      1000003 512  200 4  "-u 0.2 -q 0.2 -l 0.1 -s 14" "-u 0.2 -l 0.1 -s 15"

# --- random loss, both directions ---
run_case random_loss_10pct             1500000 1024 500 8  "-l 0.10 -s 21" "-l 0.10 -s 22"
run_case random_loss_30pct             1500000 1024 500 8  "-l 0.30 -s 23" "-l 0.30 -s 24"

# --- very high loss: must terminate, slowly is fine ---
run_case very_high_loss_50pct_completes 300000 1024 200 8  "-l 0.5 -s 31" "-l 0.5 -s 32"
run_case hopeless_loss_98pct_terminates 300000 1024 200 8  "-l 0.98 -s 33 -R 6" "-l 0.98 -s 34 -I 3" fail

# --- slow disk: writer thread falls behind, pool provides backpressure ---
run_case slow_disk_backpressure        3000000 1024 200 8  "" "-D 20000 -B 2"

echo "----------------------------------------------------------------"
echo "passed: $PASS   failed: $FAIL"
if [ $FAIL -eq 0 ]; then rm -rf "$TMP"; exit 0; else exit 1; fi

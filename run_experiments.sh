#!/usr/bin/env bash
# Throughput-vs-loss experiments (Section 6 "Experiments" of the spec doc).
#
#   main sweep      : loss 0..30%, RUNS runs per rate, 12 MiB file
#   secondary sweep : vary record size and N at a fixed loss rate
#
# usage: ./run_experiments.sh [OUTDIR]        (default OUTDIR=results)
# env  : LOSSES="0 5 10" RUNS=3 RS=1024 M=2000 N=8 RTO=50 SEC_LOSS=10 SEC=1
#        HOST=127.0.0.1  (set HOST to run the sender against a remote machine
#        where you started blast_receiver yourself - see PLAN.md)
cd "$(dirname "$0")" || exit 1
[ -x ./blast_sender ] && [ -x ./blast_receiver ] || make >/dev/null || exit 1

OUT=${1:-results}
mkdir -p "$OUT"
FILE="$OUT/testfile.bin"
[ -f "$FILE" ] || head -c 12582912 /dev/urandom > "$FILE"       # 12 MiB

LOSSES=${LOSSES:-"0 1 2 5 10 15 20 25 30"}
RUNS=${RUNS:-5}
RS=${RS:-1024}; M=${M:-2000}; N=${N:-8}; RTO=${RTO:-50}
SEC=${SEC:-1}; SEC_LOSS=${SEC_LOSS:-10}
GAP=${GAP:-0}                                # -g pacing between DATA packets (us)
SCSV="$OUT/sender.csv"; RCSV="$OUT/receiver.csv"
BAD=0

# trial TAG LOSS_FRACTION RS M N SEED_BASE
trial() {
    local tag=$1 loss=$2 rs=$3 m=$4 n=$5 seed=$6
    local port=$((20000 + RANDOM % 30000))
    mkdir -p "$OUT/recv"
    ./blast_receiver -p "$port" -d "$OUT/recv" -L 300 -I 60 -l "$loss" -s $((seed + 7000)) \
        -T "$tag" -o "$RCSV" > /dev/null 2>&1 &
    local rpid=$!
    sleep 0.2
    timeout 600 ./blast_sender -a "${HOST:-127.0.0.1}" -p "$port" -f "$FILE" -n out.bin \
        -r "$rs" -M "$m" -N "$n" -t "$RTO" -g "$GAP" -l "$loss" -s "$seed" \
        -T "$tag" -o "$SCSV" > /dev/null 2>&1
    local sc=$?
    wait "$rpid"
    if [ $sc -ne 0 ] || ! cmp -s "$FILE" "$OUT/recv/out.bin"; then
        echo "  !! trial $tag FAILED (sender rc=$sc or file mismatch)"; BAD=$((BAD + 1))
    fi
}

echo "== main sweep: file=$(stat -c %s "$FILE") bytes rs=$RS M=$M N=$N rto=${RTO}ms runs=$RUNS =="
for pct in $LOSSES; do
    loss=$(echo "scale=4; $pct / 100" | bc)
    for run in $(seq 1 "$RUNS"); do
        trial "main_l${pct}_r${run}" "$loss" "$RS" "$M" "$N" $((1000 + pct * 100 + run))
    done
    echo "  loss ${pct}% done"
done

if [ "$SEC" = 1 ]; then
    echo "== secondary sweep at ${SEC_LOSS}% loss: record size x N =="
    loss=$(echo "scale=4; $SEC_LOSS / 100" | bc)
    for rs in 256 512 1024; do
        for n in 1 2 4 8 16; do
            for run in $(seq 1 3); do
                trial "sec_rs${rs}_N${n}_r${run}" "$loss" "$rs" "$M" "$n" $((5000 + rs + n * 10 + run))
            done
        done
        echo "  rs=$rs done"
    done
fi

echo "failed trials: $BAD"
echo "raw data: $SCSV  $RCSV"
python3 ./plot.py "$OUT" || echo "(plot.py needs python3 + matplotlib; raw CSVs are in $OUT)"

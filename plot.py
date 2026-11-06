#!/usr/bin/env python3
"""Summarize BLAST experiment CSVs and plot throughput vs packet-loss rate.

usage: python3 plot.py [OUTDIR]     (default: results)

Reads  OUTDIR/sender.csv and OUTDIR/receiver.csv (rows are joined on 'tag').
Writes OUTDIR/summary_main.csv, OUTDIR/summary_secondary.csv and
       OUTDIR/throughput_vs_loss.png (needs matplotlib; the CSVs are written
       even without it).
"""
import csv
import math
import os
import sys
from collections import defaultdict


def read_csv(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def mean(xs):
    return sum(xs) / len(xs) if xs else 0.0


def std(xs):
    if len(xs) < 2:
        return 0.0
    m = mean(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))


def main():
    out = sys.argv[1] if len(sys.argv) > 1 else "results"
    srows = read_csv(os.path.join(out, "sender.csv"))
    rpath = os.path.join(out, "receiver.csv")
    rrows = {r["tag"]: r for r in read_csv(rpath)} if os.path.exists(rpath) else {}

    for r in srows:  # join receiver-side drops by tag
        rr = rrows.get(r["tag"], {})
        r["recv_dropped"] = rr.get("pkts_dropped", "0")
        r["recv_dup_records"] = rr.get("dup_records", "0")

    # ---------- main sweep: group by loss ----------
    groups = defaultdict(list)
    for r in srows:
        if r["tag"].startswith("main_"):
            groups[float(r["loss"])].append(r)

    losses = sorted(groups)
    table = []
    for l in losses:
        g = groups[l]
        thr = [float(r["throughput_MBps"]) for r in g]
        table.append({
            "loss_pct": round(l * 100, 2),
            "runs": len(g),
            "thr_mean_MBps": round(mean(thr), 3),
            "thr_std": round(std(thr), 3),
            "thr_min": round(min(thr), 3),
            "thr_max": round(max(thr), 3),
            "elapsed_mean_s": round(mean([float(r["elapsed_s"]) for r in g]), 3),
            "retx_rounds_mean": round(mean([float(r["retx_rounds"]) for r in g]), 1),
            "retx_pkts_mean": round(mean([float(r["retx_pkts"]) for r in g]), 1),
            "timeouts_mean": round(mean([float(r["timeouts"]) for r in g]), 1),
            "pkts_dropped_mean": round(mean([float(r["pkts_dropped"]) + float(r["recv_dropped"]) for r in g]), 1),
        })
    if table:
        with open(os.path.join(out, "summary_main.csv"), "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(table[0]))
            w.writeheader()
            w.writerows(table)
        print("\nMain sweep (throughput = file size / elapsed, first FILE_HDR -> sender done)")
        print(f"{'loss%':>6} {'runs':>4} {'mean MB/s':>10} {'std':>7} {'retx rounds':>12} {'timeouts':>9} {'dropped':>9}")
        for t in table:
            print(f"{t['loss_pct']:>6} {t['runs']:>4} {t['thr_mean_MBps']:>10} {t['thr_std']:>7} "
                  f"{t['retx_rounds_mean']:>12} {t['timeouts_mean']:>9} {t['pkts_dropped_mean']:>9}")

    # ---------- secondary sweep ----------
    sec = defaultdict(list)
    for r in srows:
        if r["tag"].startswith("sec_"):
            sec[(int(r["record_size"]), int(r["N"]))].append(float(r["throughput_MBps"]))
    if sec:
        with open(os.path.join(out, "summary_secondary.csv"), "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["record_size", "N", "runs", "thr_mean_MBps", "thr_std"])
            for (rs, n), v in sorted(sec.items()):
                w.writerow([rs, n, len(v), round(mean(v), 3), round(std(v), 3)])
        print("\nSecondary sweep (record size x N at fixed loss), mean MB/s")
        for (rs, n), v in sorted(sec.items()):
            print(f"  rs={rs:<5} N={n:<3} {mean(v):8.2f} +- {std(v):.2f}")

    # ---------- plot ----------
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("\nmatplotlib not installed: pip install matplotlib   (CSV summaries were written)")
        return
    if not table:
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.2))
    x = [t["loss_pct"] for t in table]
    y = [t["thr_mean_MBps"] for t in table]
    e = [t["thr_std"] for t in table]
    ax1.errorbar(x, y, yerr=e, marker="o", capsize=4, linewidth=1.8)
    for l in losses:  # individual runs as faint dots
        ax1.scatter([l * 100] * len(groups[l]), [float(r["throughput_MBps"]) for r in groups[l]],
                    s=10, alpha=0.35, color="gray", zorder=0)
    ax1.set_xlabel("packet loss rate (%)")
    ax1.set_ylabel("throughput (MB/s)")
    ax1.set_title("BLAST throughput vs packet loss (mean +- std, dots = runs)")
    ax1.grid(alpha=0.3)
    ax1.set_ylim(bottom=0)

    ax2.plot(x, [t["retx_rounds_mean"] for t in table], marker="s", label="retransmit rounds")
    ax2.plot(x, [t["timeouts_mean"] for t in table], marker="^", label="timeouts")
    ax2.set_xlabel("packet loss rate (%)")
    ax2.set_ylabel("count per run (mean)")
    ax2.set_title("Why throughput drops: recovery work")
    ax2.legend()
    ax2.grid(alpha=0.3)

    fig.tight_layout()
    png = os.path.join(out, "throughput_vs_loss.png")
    fig.savefig(png, dpi=160)
    print(f"\nwrote {png}")


if __name__ == "__main__":
    main()

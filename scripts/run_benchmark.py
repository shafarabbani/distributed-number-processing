"""Benchmark otomatis: sequential vs distributed untuk >=3 ukuran data.

Hitung Speedup = T_sequential / T_distributed dan Efficiency = Speedup/2*100%,
append hasilnya ke ``results/benchmark_results.csv``.

Catatan: dengan ``--embedded-worker`` Worker berjalan di thread pada mesin yang
sama (loopback) — cocok untuk demo/uji. Untuk angka 2 PC nyata, jalankan
``run_worker.py`` terpisah di PC2 dan JANGAN pakai flag ini.
"""

from __future__ import annotations

import argparse
import csv
import sys
import threading
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from dnp import cli
from dnp.data_generator import generate_numbers
from dnp.master import execute_distributed
from dnp.sequential import run_sequential
from dnp.worker import serve

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CSV_PATH = PROJECT_ROOT / "results" / "benchmark_results.csv"
CSV_FIELDS = [
    "data_size", "t_sequential", "t_distributed", "speedup", "efficiency",
    "compute_pc1", "compute_pc2", "send_time", "total_count",
    "total_sum", "average", "min", "max", "timestamp",
]


def run_one_size(
    size: int, seed: int, value_range: list[int],
    host: str, port: int, timeout: float,
) -> dict[str, object]:
    """Satu baris benchmark: data sama -> sequential run -> distributed run."""
    numbers = generate_numbers(size, seed, value_range)

    seq = run_sequential(numbers)
    dist = execute_distributed(numbers, host, port, timeout_seconds=timeout)

    t_seq = seq["compute_time_seconds"]
    t_dist = dist["timings"]["total_time_seconds"]
    speedup = t_seq / t_dist
    return {
        "data_size": size,
        "t_sequential": round(t_seq, 6),
        "t_distributed": round(t_dist, 6),
        "speedup": round(speedup, 4),
        "efficiency": round(speedup / 2 * 100, 2),
        "compute_pc1": round(dist["timings"]["compute_pc1_seconds"], 6),
        "compute_pc2": round(dist["timings"]["compute_pc2_seconds"], 6),
        "send_time": round(dist["timings"]["send_time_seconds"], 6),
        "total_count": dist["final"]["total_count"],
        "total_sum": dist["final"]["total_sum"],
        "average": round(dist["final"]["average"], 6),
        "min": dist["final"]["min"],
        "max": dist["final"]["max"],
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    }


def append_rows(rows: list[dict[str, object]], csv_path: Path = CSV_PATH) -> None:
    """Tulis header bila file baru; append baris hasil benchmark."""
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    is_new = not csv_path.exists()
    with csv_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        if is_new:
            writer.writeheader()
        writer.writerows(rows)


def print_summary(rows: list[dict[str, object]]) -> None:
    """Cetak tabel ringkasan speedup/efficiency ke konsol."""
    headers = ["data_size", "t_sequential", "t_distributed", "speedup", "efficiency"]
    widths = [max(len(h), *(len(str(r[h])) for r in rows)) for h in headers]
    line = "  ".join(h.ljust(w) for h, w in zip(headers, widths))
    print("\n" + line)
    print("  ".join("-" * w for w in widths))
    for row in rows:
        print("  ".join(str(row[h]).ljust(w) for h, w in zip(headers, widths)))


def main() -> int:
    """Loop benchmark untuk semua ukuran; return 0 sukses, 1 Worker tak ada."""
    preliminary = argparse.ArgumentParser(add_help=False)
    preliminary.add_argument("--config", default=None)
    known, _ = preliminary.parse_known_args()
    config = cli.load_config(known.config)

    parser = argparse.ArgumentParser(description="Benchmark distributed vs sequential.")
    parser.add_argument(
        "--sizes", type=int, nargs="+", default=config["data"]["benchmark_sizes"],
        help="daftar ukuran data (default dari config.benchmark_sizes)",
    )
    parser.add_argument("--seed", type=int, default=config["data"]["seed"])
    parser.add_argument("--worker-host", default=config["network"]["master_connect_host"])
    parser.add_argument("--port", type=int, default=config["network"]["port"])
    parser.add_argument("--timeout", type=float, default=config["network"]["timeout_seconds"])
    parser.add_argument(
        "--embedded-worker", action="store_true",
        help="jalankan Worker di thread lokal (loopback, 1 mesin); "
             "tanpa flag ini Worker harus sudah jalan di PC2",
    )
    parser.add_argument("--csv", default=str(CSV_PATH), help="path CSV output")
    parser.add_argument("--log-level", default="WARNING")
    args = parser.parse_args()

    cli.setup_logging(args.log_level)
    value_range = config["data"]["value_range"]

    stop_event = threading.Event()
    if args.embedded_worker:
        worker_thread = threading.Thread(
            target=serve,
            kwargs={
                "host": "127.0.0.1", "port": args.port,
                "timeout_seconds": args.timeout, "stop_event": stop_event,
            },
            daemon=True,
        )
        worker_thread.start()
        print(f"Embedded Worker listen di 127.0.0.1:{args.port}")

    rows: list[dict[str, object]] = []
    try:
        for size in args.sizes:
            print(f"\n=== Benchmark ukuran {size:,} angka ===")
            try:
                row = run_one_size(size, args.seed, value_range, args.worker_host, args.port, args.timeout)
            except OSError as exc:
                print(
                    f"GAGAL connect ke Worker {args.worker_host}:{args.port} -> {exc}\n"
                    "Jalankan scripts/run_worker.py di PC2 terlebih dahulu, "
                    "atau pakai --embedded-worker untuk uji 1 mesin.",
                    file=sys.stderr,
                )
                return 1
            rows.append(row)
            print(f"  T_sequential={row['t_sequential']:.4f}s  "
                  f"T_distributed={row['t_distributed']:.4f}s  "
                  f"Speedup={row['speedup']:.3f}x  Efficiency={row['efficiency']:.1f}%")
    finally:
        stop_event.set()

    append_rows(rows, Path(args.csv))
    print(f"\n{len(rows)} baris disimpan ke {args.csv}")
    print_summary(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

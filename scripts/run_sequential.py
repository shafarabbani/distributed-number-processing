"""Baseline sequential (1 PC, tanpa jaringan) — ``python scripts/run_sequential.py``."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from dnp import cli
from dnp.data_generator import generate_numbers
from dnp.sequential import run_sequential


def main() -> int:
    """Generate data (seed sama dengan mode distributed) lalu proses lokal.

    Returns:
        0 selalu.
    """
    preliminary = argparse.ArgumentParser(add_help=False)
    preliminary.add_argument("--config", default=None)
    known, _ = preliminary.parse_known_args()
    config = cli.load_config(known.config)

    args = cli.build_sequential_parser(config).parse_args()
    cli.setup_logging(args.log_level)

    print(f"Membangkitkan {args.size} angka (seed={args.seed}) ...")
    numbers = generate_numbers(args.size, args.seed, config["data"]["value_range"])

    outcome = run_sequential(numbers)
    result = outcome["result"]
    print()
    print("=" * 48)
    print(" HASIL MODE SEQUENTIAL (1 PC, tanpa jaringan)")
    print("=" * 48)
    print(f"  count      : {result['count']:,}")
    print(f"  sum        : {result['sum']:,}")
    print(f"  average    : {result['sum'] / result['count']:.4f}")
    print(f"  min        : {result['min']}")
    print(f"  max        : {result['max']}")
    print(f"  waktu      : {outcome['compute_time_seconds']:.4f} s")
    print("=" * 48)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Entry point PC1/Master — jalankan: ``python scripts/run_master.py [opsi]``."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from dnp import cli
from dnp.data_generator import generate_numbers
from dnp.master import execute_distributed, print_report


def main() -> int:
    """Parse argumen, generate data, jalankan satu run distributed.

    Returns:
        0 sukses; 1 bila Worker tidak dapat dihubungi.
    """
    preliminary = argparse.ArgumentParser(add_help=False)
    preliminary.add_argument("--config", default=None)
    known, _ = preliminary.parse_known_args()
    config = cli.load_config(known.config)

    args = cli.build_master_parser(config).parse_args()
    cli.setup_logging(args.log_level)
    value_range = config["data"]["value_range"]

    print(f"Membangkitkan {args.size} angka (seed={args.seed}) ...")
    numbers = generate_numbers(args.size, args.seed, value_range)

    try:
        report = execute_distributed(
            numbers,
            host=args.worker_host,
            port=args.port,
            timeout_seconds=args.timeout,
            split_ratio=args.split_ratio,
        )
    except OSError as exc:
        print(
            f"GAGAL: tidak dapat berkomunikasi dengan Worker "
            f"{args.worker_host}:{args.port} -> {exc}",
            file=sys.stderr,
        )
        print(
            "Pastikan run_worker.py berjalan di PC2 dan IP/port config benar.",
            file=sys.stderr,
        )
        return 1

    print_report(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

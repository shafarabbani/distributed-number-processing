"""Entry point PC2/Worker — jalankan: ``python scripts/run_worker.py [opsi]``."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from dnp import cli
from dnp.worker import serve


def main() -> int:
    """Bind listener dan layani permintaan Master dalam loop.

    Returns:
        0 selalu (berhenti lewat Ctrl+C atau setelah ``--once``).
    """
    preliminary = argparse.ArgumentParser(add_help=False)
    preliminary.add_argument("--config", default=None)
    known, _ = preliminary.parse_known_args()
    config = cli.load_config(known.config)

    args = cli.build_worker_parser(config).parse_args()
    cli.setup_logging(args.log_level)

    try:
        serve(
            host=args.host,
            port=args.port,
            timeout_seconds=args.timeout,
            once=args.once,
        )
    except KeyboardInterrupt:
        print("\nWorker dihentikan (Ctrl+C).")
    except OSError as exc:
        print(f"GAGAL bind {args.host}:{args.port} -> {exc}", file=sys.stderr)
        print("Port mungkin sudah dipakai; ubah --port atau config.yaml.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

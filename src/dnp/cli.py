"""utilitas CLI: pemuatan ``config/config.yaml``, parser argumen, dan logging.

Semua script (``scripts/run_*.py``) memakai komponen di sini supaya tidak ada
IP/port/ukuran data yang di-hardcode — nilai awal dari config, dapat
dioverride lewat argumen baris perintah (§7 AGENTS.md).
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any

import yaml

#: Direktori akar proyek (berisi ``config/``, ``src/``, dst.).
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"


def load_config(config_path: str | Path | None = None) -> dict[str, Any]:
    """Baca dan validasi file konfigurasi YAML.

    Args:
        config_path: Lokasi config; ``None`` = ``config/config.yaml`` proyek.

    Returns:
        Dict konfigurasi dengan kunci ``network`` dan ``data``.

    Raises:
        FileNotFoundError: File config tidak ada.
        ValueError: YAML tidak valid atau bagian wajib hilang.
    """
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    if not path.is_file():
        raise FileNotFoundError(f"file konfigurasi tidak ditemukan: {path}")
    with path.open(encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    if not isinstance(config, dict):
        raise ValueError(f"config harus berupa mapping YAML: {path}")
    for section in ("network", "data"):
        if section not in config:
            raise ValueError(f"bagian wajib '{section}' hilang dari {path}")
    return config


def setup_logging(level: str = "INFO") -> None:
    """Konfigurasi logging root dengan format ringkas untuk konsol.

    Args:
        level: Nama level (DEBUG/INFO/WARNING/ERROR).
    """
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def _add_network_args(parser: argparse.ArgumentParser, config: dict[str, Any]) -> None:
    network = config["network"]
    parser.add_argument(
        "--worker-host",
        default=network["master_connect_host"],
        help=f"IP/name Worker (default dari config: {network['master_connect_host']})",
    )
    parser.add_argument(
        "--port", type=int, default=network["port"],
        help=f"port TCP (default dari config: {network['port']})",
    )
    parser.add_argument(
        "--timeout", type=float, default=network["timeout_seconds"],
        help=f"timeout socket detik (default dari config: {network['timeout_seconds']})",
    )


def build_master_parser(config: dict[str, Any]) -> argparse.ArgumentParser:
    """Parser argumen untuk ``run_master.py`` (mode distributed)."""
    parser = argparse.ArgumentParser(
        description="PC1/Master: proses angka secara terdistribusi bersama PC2/Worker."
    )
    parser.add_argument(
        "--size", type=int, default=config["data"]["default_size"],
        help="jumlah angka yang dibangkitkan",
    )
    parser.add_argument(
        "--seed", type=int, default=config["data"]["seed"], help="seed generator data"
    )
    parser.add_argument(
        "--split-ratio", type=float, default=0.5,
        help="proporsi beban PC1, 0..1 (default 0.5)",
    )
    parser.add_argument("--config", default=None, help="path config.yaml alternatif")
    parser.add_argument("--log-level", default="INFO", help="level logging")
    _add_network_args(parser, config)
    return parser


def build_worker_parser(config: dict[str, Any]) -> argparse.ArgumentParser:
    """Parser argumen untuk ``run_worker.py``."""
    network = config["network"]
    parser = argparse.ArgumentParser(
        description="PC2/Worker: server TCP pemroses porsi angka dari Master."
    )
    parser.add_argument(
        "--host", default=network["worker_host"],
        help=f"alamat bind (default dari config: {network['worker_host']})",
    )
    parser.add_argument(
        "--port", type=int, default=network["port"],
        help=f"port listen (default dari config: {network['port']})",
    )
    parser.add_argument(
        "--timeout", type=float, default=network["timeout_seconds"],
        help="timeout socket detik",
    )
    parser.add_argument(
        "--once", action="store_true", help="layani satu request lalu exit"
    )
    parser.add_argument("--config", default=None, help="path config.yaml alternatif")
    parser.add_argument("--log-level", default="INFO", help="level logging")
    return parser


def build_sequential_parser(config: dict[str, Any]) -> argparse.ArgumentParser:
    """Parser argumen untuk ``run_sequential.py`` (baseline)."""
    parser = argparse.ArgumentParser(
        description="Baseline sequential: proses seluruh angka di 1 PC tanpa jaringan."
    )
    parser.add_argument(
        "--size", type=int, default=config["data"]["default_size"],
        help="jumlah angka yang dibangkitkan",
    )
    parser.add_argument(
        "--seed", type=int, default=config["data"]["seed"], help="seed generator data"
    )
    parser.add_argument("--config", default=None, help="path config.yaml alternatif")
    parser.add_argument("--log-level", default="INFO", help="level logging")
    return parser

"""Baseline sequential (1 PC, tanpa jaringan) sebagai pembanding speedup.

Proses SELURUH daftar angka dalam satu fungsi :func:`process_numbers` —
tidak ada split, tidak ada socket.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Sequence

from dnp.number_processor import process_numbers

logger = logging.getLogger("dnp.sequential")


def run_sequential(numbers: Sequence[float]) -> dict[str, Any]:
    """Hitung agregat seluruh data di satu proses dan ukur waktunya.

    Args:
        numbers: Seluruh angka (daftar yang sama persis dengan yang dipakai
            mode distributed, agar perbandingan adil).

    Returns:
        Dict berisi ``result`` (keluaran :func:`process_numbers`) dan
        ``compute_time_seconds`` (waktu komputasi murni, tanpa generate data).

    Raises:
        ValueError: Data kosong.
    """
    t_start = time.perf_counter()
    result = process_numbers(numbers)
    compute_time = time.perf_counter() - t_start
    logger.info("sequential selesai: %d angka dalam %.4f s", result["count"], compute_time)
    return {"result": result, "compute_time_seconds": compute_time}

"""Modul komputasi murni: agregasi angka dan penggabungan hasil parsial.

Sengaja TIDAK mengenal socket/networking sama sekali (prinsip §2 AGENTS.md),
sehingga seluruh logikanya bisa diuji tanpa jaringan.
"""

from __future__ import annotations

from typing import Any, Sequence

#: Kunci wajib pada hasil parsial keluaran :func:`process_numbers`.
PARTIAL_KEYS = ("count", "sum", "min", "max")


def process_numbers(numbers: Sequence[float]) -> dict[str, Any]:
    """Hitung agregat lokal (sum, count, min, max) dari satu porsi angka.

    Args:
        numbers: Urutan angka yang akan diproses.

    Returns:
        Dict dengan kunci ``count`` (jumlah elemen), ``sum`` (total nilai),
        ``min`` dan ``max``.

    Raises:
        ValueError: Bila ``numbers`` kosong — min/max tidak terdefinisi untuk
            himpunan kosong, jadi kasus ini ditangani eksplisit.
    """
    count = len(numbers)
    if count == 0:
        raise ValueError("process_numbers menerima list kosong; minimal 1 angka.")
    return {
        "count": count,
        "sum": sum(numbers),
        "min": min(numbers),
        "max": max(numbers),
    }


def merge_results(result_a: dict[str, Any], result_b: dict[str, Any]) -> dict[str, Any]:
    """Gabungkan dua hasil parsial menjadi hasil akhir.

    Mengandalkan sifat combinable langsung dari sum/count/min/max:
        total_sum   = sum_a + sum_b
        total_count = count_a + count_b
        average     = total_sum / total_count
        min         = min(min_a, min_b)
        max         = max(max_a, max_b)

    Args:
        result_a: Hasil parsial pertama (keluaran :func:`process_numbers`).
        result_b: Hasil parsial kedua.

    Returns:
        Dict dengan kunci ``total_count``, ``total_sum``, ``average``,
        ``min``, ``max``.

    Raises:
        ValueError: Bila salah satu dict tidak memiliki kunci wajib
            (:data:`PARTIAL_KEYS`) atau ``total_count``-nya nol.
    """
    for name, result in (("result_a", result_a), ("result_b", result_b)):
        missing = [key for key in PARTIAL_KEYS if key not in result]
        if missing:
            raise ValueError(f"{name} tidak memiliki kunci wajib: {missing}")

    total_count = result_a["count"] + result_b["count"]
    total_sum = result_a["sum"] + result_b["sum"]
    if total_count == 0:
        raise ValueError("Kedua hasil parsial kosong; average tidak terdefinisi.")
    return {
        "total_count": total_count,
        "total_sum": total_sum,
        "average": total_sum / total_count,
        "min": min(result_a["min"], result_b["min"]),
        "max": max(result_a["max"], result_b["max"]),
    }


def split_numbers(numbers: Sequence[float], split_ratio: float = 0.5) -> tuple[list[float], list[float]]:
    """Bagi daftar angka menjadi dua bagian near-equal untuk PC1 dan PC2.

    Args:
        numbers: Daftar angka utuh.
        split_ratio: Proporsi bagian pertama (default 50:50). Nilai valid di
            rentang (0, 1); boleh digeser untuk eksperimen load balancing.

    Returns:
        Tuple ``(bagian_pc1, bagian_pc2)``.

    Raises:
        ValueError: Bila ``split_ratio`` di luar (0, 1) atau data terlalu
            sedikit untuk dibagi dua (kurang dari 2 elemen).
    """
    if not 0.0 < split_ratio < 1.0:
        raise ValueError(f"split_ratio harus di (0, 1), diterima: {split_ratio}")
    if len(numbers) < 2:
        raise ValueError("split_numbers membutuhkan minimal 2 angka.")
    index = round(len(numbers) * split_ratio)
    index = max(1, min(index, len(numbers) - 1))
    return list(numbers[:index]), list(numbers[index:])

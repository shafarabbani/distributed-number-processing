"""Pembangkit daftar angka uji yang deterministik (reproducible via seed)."""

from __future__ import annotations

import random
from typing import Sequence


def generate_numbers(
    size: int,
    seed: int,
    value_range: Sequence[int] = (1, 1_000_000),
) -> list[int]:
    """Bangkitkan ``size`` angka bulat acak dalam rentang inklusif.

    Args:
        size: Banyaknya angka yang dihasilkan (>= 1).
        seed: Seed RNG. Seed yang sama selalu menghasilkan daftar yang sama,
            sehingga benchmark distributed vs sequential memakai data identik.
        value_range: Pasang ``(min, max)`` nilai, inklusif.

    Returns:
        Daftar ``size`` bilangan bulat.

    Raises:
        ValueError: Bila ``size < 1`` atau rentang tidak valid (min > max).
    """
    if size < 1:
        raise ValueError(f"size harus >= 1, diterima: {size}")
    low, high = int(value_range[0]), int(value_range[1])
    if low > high:
        raise ValueError(f"value_range tidak valid: {value_range}")
    rng = random.Random(seed)
    return [rng.randint(low, high) for _ in range(size)]

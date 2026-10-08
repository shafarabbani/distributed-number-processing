"""Unit test untuk dnp.data_generator."""

from __future__ import annotations

import pytest

from dnp.data_generator import generate_numbers


class TestSize:
    def test_returns_requested_size(self) -> None:
        assert len(generate_numbers(1000, seed=1)) == 1000

    def test_single_number(self) -> None:
        assert len(generate_numbers(1, seed=1)) == 1

    @pytest.mark.parametrize("bad_size", [0, -5])
    def test_invalid_size_raises(self, bad_size: int) -> None:
        with pytest.raises(ValueError):
            generate_numbers(bad_size, seed=1)


class TestDeterminism:
    def test_same_seed_same_data(self) -> None:
        assert generate_numbers(500, seed=42) == generate_numbers(500, seed=42)

    def test_different_seed_different_data(self) -> None:
        assert generate_numbers(500, seed=42) != generate_numbers(500, seed=43)


class TestValueRange:
    def test_values_within_range(self) -> None:
        data = generate_numbers(2000, seed=7, value_range=(10, 20))
        assert all(10 <= value <= 20 for value in data)

    def test_invalid_range_raises(self) -> None:
        with pytest.raises(ValueError):
            generate_numbers(10, seed=1, value_range=(100, 1))

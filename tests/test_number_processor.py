"""Unit test untuk dnp.number_processor."""

from __future__ import annotations

import math

import pytest

from dnp.data_generator import generate_numbers
from dnp.number_processor import merge_results, process_numbers, split_numbers


class TestProcessNumbers:
    def test_empty_list_raises(self) -> None:
        with pytest.raises(ValueError):
            process_numbers([])

    def test_single_element(self) -> None:
        assert process_numbers([7.5]) == {"count": 1, "sum": 7.5, "min": 7.5, "max": 7.5}

    def test_many_elements(self) -> None:
        result = process_numbers([3, 1, 4, 1, 5, 9, 2, 6])
        assert result == {"count": 8, "sum": 31, "min": 1, "max": 9}

    def test_negative_numbers(self) -> None:
        result = process_numbers([-10, -3, -7])
        assert result == {"count": 3, "sum": -20, "min": -10, "max": -3}

    def test_float_values(self) -> None:
        result = process_numbers([0.5, 1.25, -2.0])
        assert result["count"] == 3
        assert math.isclose(result["sum"], -0.25)
        assert math.isclose(result["min"], -2.0)
        assert math.isclose(result["max"], 1.25)


class TestSplitNumbers:
    def test_even_split_balanced(self) -> None:
        left, right = split_numbers(list(range(100)))
        assert len(left) == 50 and len(right) == 50

    def test_odd_split_near_equal(self) -> None:
        left, right = split_numbers(list(range(11)))
        assert {len(left), len(right)} == {5, 6}

    def test_split_preserves_all_elements(self) -> None:
        data = list(range(1000))
        left, right = split_numbers(data)
        assert left + right == data

    @pytest.mark.parametrize("ratio", [0.0, 1.0, -0.5, 1.5])
    def test_invalid_ratio_raises(self, ratio: float) -> None:
        with pytest.raises(ValueError):
            split_numbers(list(range(10)), ratio)

    def test_too_few_elements_raises(self) -> None:
        with pytest.raises(ValueError):
            split_numbers([1])


class TestMergeCorrectness:
    """Inti bukti korektnya skema merge (AGENTS.md §5)."""

    @pytest.mark.parametrize(
        "size,seed",
        [(10, 1), (997, 2), (10_000, 3)],
    )
    def test_merge_equals_direct_computation(self, size: int, seed: int) -> None:
        data = generate_numbers(size, seed)
        part_a, part_b = split_numbers(data)

        merged = merge_results(process_numbers(part_a), process_numbers(part_b))
        direct = process_numbers(data)

        assert merged["total_count"] == direct["count"]
        assert merged["total_sum"] == direct["sum"]
        assert merged["min"] == direct["min"]
        assert merged["max"] == direct["max"]
        assert math.isclose(
            merged["average"], direct["sum"] / direct["count"], rel_tol=1e-12
        )

    def test_average_formula(self) -> None:
        result_a = process_numbers([2, 4])
        result_b = process_numbers([6, 8])
        merged = merge_results(result_a, result_b)
        assert merged["total_sum"] == 20
        assert merged["total_count"] == 4
        assert merged["average"] == 5.0

    def test_missing_key_raises(self) -> None:
        with pytest.raises(ValueError):
            merge_results({"sum": 1}, {"count": 1, "sum": 1, "min": 1, "max": 1})

    def test_both_empty_counts_raises(self) -> None:
        empty_partial = {"count": 0, "sum": 0, "min": 0, "max": 0}
        with pytest.raises(ValueError):
            merge_results(empty_partial, dict(empty_partial))

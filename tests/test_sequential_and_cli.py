"""Test modul pendukung: sequential baseline, utilitas CLI/config, report master."""

from __future__ import annotations

import pytest

from dnp import cli
from dnp.data_generator import generate_numbers
from dnp.master import print_report
from dnp.number_processor import process_numbers
from dnp.sequential import run_sequential


class TestRunSequential:
    def test_matches_process_numbers_and_returns_time(self) -> None:
        numbers = generate_numbers(500, seed=3)
        outcome = run_sequential(numbers)
        assert outcome["result"] == process_numbers(numbers)
        assert outcome["compute_time_seconds"] >= 0.0

    def test_empty_raises(self) -> None:
        with pytest.raises(ValueError):
            run_sequential([])


class TestLoadConfig:
    def test_default_config_has_required_sections(self) -> None:
        config = cli.load_config()
        assert set(config["network"]) >= {"worker_host", "master_connect_host", "port", "timeout_seconds"}
        assert set(config["data"]) >= {"default_size", "seed", "benchmark_sizes", "value_range"}

    def test_missing_file_raises(self, tmp_path) -> None:
        with pytest.raises(FileNotFoundError):
            cli.load_config(tmp_path / "tidak_ada.yaml")

    def test_invalid_yaml_missing_section_raises(self, tmp_path) -> None:
        bad = tmp_path / "bad.yaml"
        bad.write_text("network:\n  port: 5050\n", encoding="utf-8")
        with pytest.raises(ValueError):
            cli.load_config(bad)


class TestParsers:
    def test_master_parser_defaults_from_config(self) -> None:
        config = cli.load_config()
        args = cli.build_master_parser(config).parse_args([])
        assert args.size == config["data"]["default_size"]
        assert args.port == config["network"]["port"]

    def test_worker_parser_override(self) -> None:
        config = cli.load_config()
        args = cli.build_worker_parser(config).parse_args(["--host", "0.0.0.0", "--once"])
        assert args.host == "0.0.0.0" and args.once is True


class TestPrintReport:
    def test_output_contains_all_required_sections(self, capsys) -> None:
        report = {
            "request_id": "x",
            "total_count": 4,
            "load": {"pc1": 2, "pc2": 2},
            "pc1_result": {"count": 2, "sum": 6, "min": 2, "max": 4},
            "pc2_result": {"count": 2, "sum": 14, "min": 6, "max": 8},
            "final": {"total_count": 4, "total_sum": 20, "average": 5.0, "min": 2, "max": 8},
            "timings": {
                "connect_time_seconds": 0.0, "send_time_seconds": 0.0,
                "receive_time_seconds": 0.0, "compute_pc1_seconds": 0.0,
                "compute_pc2_seconds": 0.0, "merge_time_seconds": 0.0,
                "total_time_seconds": 0.0,
            },
        }
        print_report(report)
        out = capsys.readouterr().out
        for marker in (
            "Jumlah total angka", "Beban per PC", "Hasil komputasi PC1",
            "Hasil komputasi PC2", "Hasil akhir gabungan", "Rincian waktu",
            "20", "5.0000",
        ):
            assert marker in out

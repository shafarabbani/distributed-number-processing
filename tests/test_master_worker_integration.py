"""Test integrasi master <-> worker via loopback (127.0.0.1).

Worker dijalankan di thread terpisah memakai port bebas (``port=0``),
lalu alur master penuh diverifikasi terhadap perhitungan langsung
atas seluruh data.
"""

from __future__ import annotations

import threading
from queue import Empty, Queue

import pytest

from dnp.data_generator import generate_numbers
from dnp.master import execute_distributed
from dnp.number_processor import process_numbers
from dnp.protocol import send_message, receive_message, MESSAGE_TYPES
from dnp.worker import serve


@pytest.fixture
def worker_port():
    """Nyalakan Worker di thread dengan port bebas; yield port aktual."""
    ready: Queue[int] = Queue()
    stop_event = threading.Event()
    errors: list[BaseException] = []

    def runner() -> None:
        try:
            serve(
                host="127.0.0.1", port=0, timeout_seconds=5.0,
                stop_event=stop_event, on_ready=ready.put,
            )
        except BaseException as exc:  # pragma: no cover - hanya bila bug thread mati
            errors.append(exc)

    thread = threading.Thread(target=runner, daemon=True)
    thread.start()
    port = ready.get(timeout=5)
    yield port
    stop_event.set()
    thread.join(timeout=5)
    assert not errors, f"thread Worker crash: {errors}"


class TestDistributedMatchesDirect:
    def test_full_flow_matches_direct_computation(self, worker_port: int) -> None:
        numbers = generate_numbers(5_000, seed=42)
        direct = process_numbers(numbers)

        report = execute_distributed(numbers, "127.0.0.1", worker_port, timeout_seconds=5)
        final = report["final"]

        assert final["total_count"] == direct["count"]
        assert final["total_sum"] == direct["sum"]
        assert final["min"] == direct["min"]
        assert final["max"] == direct["max"]
        assert final["average"] == pytest.approx(direct["sum"] / direct["count"])

    def test_load_split_counts(self, worker_port: int) -> None:
        numbers = generate_numbers(101, seed=7)
        report = execute_distributed(numbers, "127.0.0.1", worker_port, timeout_seconds=5)
        assert report["load"]["pc1"] + report["load"]["pc2"] == 101
        assert report["pc1_result"]["count"] == report["load"]["pc1"]
        assert report["pc2_result"]["count"] == report["load"]["pc2"]

    def test_multiple_requests_on_one_worker(self, worker_port: int) -> None:
        """Worker loop harus melayani beberapa run berturut-turut tanpa crash."""
        for seed in (1, 2, 3):
            numbers = generate_numbers(200, seed=seed)
            direct = process_numbers(numbers)
            report = execute_distributed(numbers, "127.0.0.1", worker_port, timeout_seconds=5)
            assert report["final"]["total_sum"] == direct["sum"]

    def test_custom_split_ratio(self, worker_port: int) -> None:
        numbers = generate_numbers(1_000, seed=9)
        report = execute_distributed(
            numbers, "127.0.0.1", worker_port, timeout_seconds=5, split_ratio=0.3
        )
        assert report["load"]["pc1"] == 300
        assert report["load"]["pc2"] == 700
        assert report["final"]["total_sum"] == process_numbers(numbers)["sum"]


class TestWorkerPingPong:
    def test_ping_gets_pong(self, worker_port: int) -> None:
        import socket

        with socket.create_connection(("127.0.0.1", worker_port), timeout=5) as sock:
            send_message(sock, {"type": "ping"})
            reply = receive_message(sock)
        assert reply == {"type": "pong"}


class TestWorkerErrorHandling:
    def test_unknown_message_type_logged_not_crash(self, worker_port: int) -> None:
        """Pesan rusak -> koneksi ditutup/ditolak, Worker tetap hidup."""
        import socket

        with socket.create_connection(("127.0.0.1", worker_port), timeout=5) as sock:
            send_message(sock, {"type": "not_a_real_type"})
        # Worker tetap siap untuk permintaan berikutnya:
        numbers = generate_numbers(50, seed=1)
        report = execute_distributed(numbers, "127.0.0.1", worker_port, timeout_seconds=5)
        assert report["final"]["total_count"] == 50


@pytest.mark.parametrize("bad_type", ["data_chunk_missing", "", "execute"])
def test_protocol_message_type_set_unchanged(bad_type: str) -> None:
    assert bad_type not in MESSAGE_TYPES
    with pytest.raises(ValueError):
        from dnp.protocol import validate_message_type

        validate_message_type({"type": bad_type})


class TestUnreachableWorker:
    def test_master_raises_on_dead_worker(self) -> None:
        """Tanpa Worker, master harus gagal dengan error jaringan jelas."""
        import socket

        # Bind port lalu tutup -> port praktis kosong.
        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        dead_port = probe.getsockname()[1]
        probe.close()

        numbers = generate_numbers(10, seed=1)
        with pytest.raises(OSError):
            execute_distributed(numbers, "127.0.0.1", dead_port, timeout_seconds=1)

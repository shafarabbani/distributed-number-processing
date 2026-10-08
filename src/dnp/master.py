"""Logika PC1 (Master/Coordinator).

Alur (§6 AGENTS.md): split data 50:50 -> bagian PC2 dikirim ke Worker secara
konkuren (thread) SAMPING PC1 memproses bagiannya sendiri -> terima hasil
Worker -> merge -> laporan + tabel output wajib ke layar.

Pemilihan konkurensi: ``concurrent.futures.ThreadPoolExecutor``. GIL tidak
menjadi masalah karena pekerjaan berlangsung bersamaan antara I/O jaringan
(thread) dan komputasi Python (main thread), sehingga waktu kirim/receive
overlap dengan komputasi lokal — bukan sekuensial kirim-lalu-hitung.
"""

from __future__ import annotations

import logging
import socket
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Sequence

from dnp import protocol
from dnp.number_processor import merge_results, process_numbers, split_numbers

logger = logging.getLogger("dnp.master")


def _dispatch_to_worker(
    host: str,
    port: int,
    timeout_seconds: float,
    numbers: Sequence[float],
) -> dict[str, Any]:
    """Kirim satu ``data_chunk`` ke Worker dan tunggu ``process_result``.

    Runs di thread terpisah dari komputasi lokal Master.

    Returns:
        Dict mentah berisi hasil Worker + waktu koneksi/kirim/terima.

    Raises:
        ConnectionError: Worker membalas dengan tipe pesan yang salah.
        OSError/socket.timeout: Kegagalan jaringan.
    """
    request_id = uuid.uuid4().hex
    timings: dict[str, float] = {}

    t0 = time.perf_counter()
    with socket.create_connection((host, port), timeout=timeout_seconds) as sock:
        timings["connect_time_seconds"] = time.perf_counter() - t0
        sock.settimeout(timeout_seconds)

        message = {"type": "data_chunk", "request_id": request_id, "data": list(numbers)}
        t1 = time.perf_counter()
        protocol.send_message(sock, message)
        timings["send_time_seconds"] = time.perf_counter() - t1

        reply = protocol.receive_message(sock)
        timings["receive_time_seconds"] = time.perf_counter() - t1
        message_type = protocol.validate_message_type(reply)
        if message_type != "process_result":
            raise ConnectionError(f"Master menunggu process_result, diterima: {message_type}")
        if reply.get("request_id") != request_id:
            raise ConnectionError("request_id balasan Worker tidak cocok")

    return {
        "worker_result": reply["result"],
        "worker_compute_time_seconds": reply["compute_time_seconds"],
        **timings,
    }


def execute_distributed(
    numbers: Sequence[float],
    host: str,
    port: int,
    timeout_seconds: float = 30.0,
    split_ratio: float = 0.5,
) -> dict[str, Any]:
    """Jalankan seluruh alur distributed satu kali dan kembalikan laporannya.

    Args:
        numbers: Seluruh angka yang akan diproses terdistribusi.
        host: Alamat Worker (PC2).
        port: Port Worker.
        timeout_seconds: Timeout socket.
        split_ratio: Proporsi beban PC1 (default 0.5).

    Returns:
        Laporan dict: beban tiap PC, hasil parsial PC1/PC2, hasil gabungan,
        dan rincian waktu (connect, kirim, terima, komputasi PC1/PC2, merge,
        total end-to-end).

    Raises:
        ValueError: Data terlalu kecil untuk di-split.
        ConnectionError/OSError/socket.timeout: Worker tidak merespons.
    """
    chunk_pc1, chunk_pc2 = split_numbers(numbers, split_ratio)
    request_id = uuid.uuid4().hex
    t_start = time.perf_counter()
    logger.info(
        "start distributed run id=%s: total=%d (PC1=%d, PC2=%d)",
        request_id, len(chunk_pc1) + len(chunk_pc2), len(chunk_pc1), len(chunk_pc2),
    )

    with ThreadPoolExecutor(max_workers=1) as pool:
        # Kirim ke Worker di thread lain, SAMBIL PC1 menghitung porsinya.
        future: Future = pool.submit(
            _dispatch_to_worker, host, port, timeout_seconds, chunk_pc2
        )
        t_local = time.perf_counter()
        result_pc1 = process_numbers(chunk_pc1)
        local_compute_time = time.perf_counter() - t_local
        remote = future.result()  # tunggu Worker sampai process_result tiba

    t_merge = time.perf_counter()
    merged = merge_results(result_pc1, remote["worker_result"])
    merge_time = time.perf_counter() - t_merge
    total_time = time.perf_counter() - t_start

    report = {
        "request_id": request_id,
        "total_count": merged["total_count"],
        "load": {"pc1": len(chunk_pc1), "pc2": len(chunk_pc2)},
        "pc1_result": result_pc1,
        "pc2_result": remote["worker_result"],
        "final": merged,
        "timings": {
            "connect_time_seconds": remote["connect_time_seconds"],
            "send_time_seconds": remote["send_time_seconds"],
            "receive_time_seconds": remote["receive_time_seconds"],
            "compute_pc1_seconds": local_compute_time,
            "compute_pc2_seconds": remote["worker_compute_time_seconds"],
            "merge_time_seconds": merge_time,
            "total_time_seconds": total_time,
        },
    }
    logger.info("selesai: total_time=%.4f s", total_time)
    return report


def _format_table(title: str, rows: list[tuple[str, str]]) -> str:
    """Render baris (kunci, nilai) jadi tabel teks rata kiri."""
    key_width = max(len(key) for key, _ in rows)
    lines = [title, "-" * (len(title))]
    lines += [f"  {key.ljust(key_width)}  {value}" for key, value in rows]
    return "\n".join(lines)


def print_report(report: dict[str, Any]) -> None:
    """Cetak output wajib PLAN.md §7 ke layar dalam format tabel teks."""
    load = report["load"]
    pc1 = report["pc1_result"]
    pc2 = report["pc2_result"]
    final = report["final"]
    timings = report["timings"]

    print()
    print("=" * 64)
    print(" HASIL DISTRIBUTED NUMBER PROCESSING (PC1 + PC2)")
    print("=" * 64)
    print(_format_table("1. Jumlah total angka yang diproses", [
        ("total", str(final["total_count"])),
    ]))
    print(_format_table("2. Beban per PC", [
        ("PC1 (Master)", f"{load['pc1']} angka ({load['pc1'] / final['total_count'] * 100:.1f}%)"),
        ("PC2 (Worker)", f"{load['pc2']} angka ({load['pc2'] / final['total_count'] * 100:.1f}%)"),
    ]))
    print(_format_table("3. Hasil komputasi PC1 (parsial)", [
        ("sum", f"{pc1['sum']:,}"), ("count", f"{pc1['count']:,}"),
        ("min", str(pc1["min"])), ("max", str(pc1["max"])),
    ]))
    print(_format_table("4. Hasil komputasi PC2 (parsial)", [
        ("sum", f"{pc2['sum']:,}"), ("count", f"{pc2['count']:,}"),
        ("min", str(pc2["min"])), ("max", str(pc2["max"])),
    ]))
    print(_format_table("5. Hasil akhir gabungan", [
        ("total_sum", f"{final['total_sum']:,}"),
        ("total_count", f"{final['total_count']:,}"),
        ("average", f"{final['average']:.4f}"),
        ("min", str(final["min"])),
        ("max", str(final["max"])),
    ]))
    print(_format_table("6. Rincian waktu eksekusi (detik)", [
        ("connect", f"{timings['connect_time_seconds']:.4f}"),
        ("kirim data ke PC2", f"{timings['send_time_seconds']:.4f}"),
        ("terima hasil dari PC2", f"{timings['receive_time_seconds']:.4f}"),
        ("komputasi PC1", f"{timings['compute_pc1_seconds']:.4f}"),
        ("komputasi PC2", f"{timings['compute_pc2_seconds']:.4f}"),
        ("merge hasil", f"{timings['merge_time_seconds']:.4f}"),
        ("TOTAL end-to-end", f"{timings['total_time_seconds']:.4f}"),
    ]))
    print("=" * 64)

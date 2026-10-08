"""Logika PC2 (Worker): server TCP yang memproses porsi angka dari Master.

Alur per koneksi (§6 AGENTS.md): terima ``data_chunk`` -> hitung
``process_numbers()`` -> balas ``process_result`` (hasil + waktu komputasi
lokal). Loop server membuat Worker siap menerima permintaan berikutnya tanpa
restart, kecuali dijalankan dengan ``once=True``/``max_requests``.
"""

from __future__ import annotations

import logging
import socket
import threading
import time
from typing import Any, Callable

from dnp import protocol
from dnp.number_processor import process_numbers

logger = logging.getLogger("dnp.worker")


def handle_connection(conn: socket.socket) -> dict[str, Any] | None:
    """Layani satu koneksi Master: baca satu pesan, balas seperlunya.

    Args:
        conn: Socket koneksi yang sudah diterima dari listener.

    Returns:
        Salinan ``process_result`` yang dikirim (untuk pengujian/logging),
        atau ``None`` bila pesan hanya handshake ``ping``.

    Raises:
        ValueError: Tipe pesan tidak dikenal.
        ConnectionError / OSError / socket.timeout: Masalah jaringan.
    """
    message = protocol.receive_message(conn)
    message_type = protocol.validate_message_type(message)

    if message_type == "ping":
        protocol.send_message(conn, {"type": "pong"})
        logger.info("ping diterima -> pong dikirim")
        return None

    if message_type != "data_chunk":
        raise ValueError(f"Worker menunggu data_chunk, diterima: {message_type}")

    numbers = message.get("data")
    if not isinstance(numbers, list):
        raise ValueError("pesan data_chunk tidak memiliki kunci 'data' berupa list")

    logger.info("menerima %d angka dari Master", len(numbers))
    t_start = time.perf_counter()
    result = process_numbers(numbers)
    compute_time = time.perf_counter() - t_start

    reply = {
        "type": "process_result",
        "request_id": message.get("request_id"),
        "result": result,
        "compute_time_seconds": compute_time,
    }
    protocol.send_message(conn, reply)
    logger.info(
        "selesai: sum=%.1f count=%d min=%s max=%s (komputasi lokal %.4f s)",
        result["sum"], result["count"], result["min"], result["max"], compute_time,
    )
    return reply


def serve(
    host: str,
    port: int,
    timeout_seconds: float,
    once: bool = False,
    max_requests: int | None = None,
    stop_event: threading.Event | None = None,
    on_ready: Callable[[int], None] | None = None,
) -> int:
    """Jalankan loop server Worker; kembalikan jumlah permintaan yang dilayani.

    Args:
        host: Alamat bind (``0.0.0.0`` untuk semua interface).
        port: Port listen; ``0`` memakai port bebas (untuk pengujian).
        timeout_seconds: Timeout operasi baca/tulis per koneksi.
        once: ``True`` = layani satu koneksi lalu berhenti (flag ``--once``).
        max_requests: Batas jumlah permintaan; ``None`` = tak terbatas.
        stop_event: Bila diisi, loop berhenti saat event di-set (untuk uji).
        on_ready: Dipanggil dengan port aktual setelah bind berhasil.

    Returns:
        Banyaknya koneksi yang berhasil dilayani.
    """
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((host, port))
    listener.listen(1)
    actual_port = listener.getsockname()[1]
    listener.settimeout(0.5)  # timeout pendek agar stop_event tetap responsif
    logger.info("Worker listen di %s:%d", host, actual_port)
    if on_ready is not None:
        on_ready(actual_port)

    served = 0
    limit = 1 if once else max_requests
    try:
        while limit is None or served < limit:
            if stop_event is not None and stop_event.is_set():
                break
            try:
                conn, peer = listener.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            logger.info("koneksi dari %s:%s", *peer[:2])
            conn.settimeout(timeout_seconds)
            try:
                with conn:
                    handle_connection(conn)
                served += 1
            except ValueError as exc:
                logger.error("pesan tidak valid dari %s:%s: %s", *peer[:2], exc)
            except (ConnectionError, OSError) as exc:
                logger.error("koneksi dari %s:%s terputus: %s", *peer[:2], exc)
    finally:
        listener.close()
        logger.info("Worker berhenti setelah melayani %d permintaan", served)
    return served

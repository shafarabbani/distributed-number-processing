"""Protokol komunikasi: framing + (de)serialisasi pesan JSON di atas TCP.

Setiap pesan = header 4 byte big-endian (panjang payload) + payload JSON UTF-8.
Modul ini hanya mengurus byte di wire, TIDAK tahu rumus perhitungan apa pun.
"""

from __future__ import annotations

import json
import socket
import struct
from typing import Any

#: Format header: unsigned int big-endian, 4 byte.
HEADER_FORMAT = "!I"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

#: Ukuran pesan maksimum yang diizinkan header 4 byte.
MAX_MESSAGE_BYTES = 2**32 - 1

#: Ukuran potongan baca per panggilan ``recv`` (byte).
_RECV_CHUNK = 65536

#: Jenis pesan yang dikenal protokol.
MESSAGE_TYPES = frozenset({"data_chunk", "process_result", "ping", "pong"})


def send_message(sock: socket.socket, payload: dict[str, Any]) -> None:
    """Kirim satu pesan dict sebagai `4-byte length header + JSON UTF-8`.

    Args:
        sock: Socket TCP yang sudah terhubung.
        payload: Dict yang bisa diserialisasi JSON; wajib punya kunci
            ``"type"`` salah satu dari :data:`MESSAGE_TYPES`.

    Raises:
        ValueError: Payload bukan dict, tidak bisa di-serialisasi JSON, atau
            melebihi kapasitas header.
        OSError: Kegagalan pengiriman di level socket.
    """
    if not isinstance(payload, dict):
        raise ValueError(f"payload harus dict, diterima {type(payload).__name__}")
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    if len(body) > MAX_MESSAGE_BYTES:
        raise ValueError(f"payload {len(body)} byte melebihi batas {MAX_MESSAGE_BYTES} byte")
    sock.sendall(struct.pack(HEADER_FORMAT, len(body)) + body)


def _recv_exactly(sock: socket.socket, count: int) -> bytes:
    """Baca tepat ``count`` byte dari stream socket.

    Raises:
        ConnectionError: Koneksi ditutup peer sebelum semua byte diterima.
        OSError: Kegagalan lain di level socket.
    """
    chunks: list[bytes] = []
    remaining = count
    while remaining > 0:
        chunk = sock.recv(min(_RECV_CHUNK, remaining))
        if not chunk:
            raise ConnectionError(
                f"koneksi ditutup sebelum pesan lengkap: {count - remaining}/{count} byte diterima"
            )
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def receive_message(sock: socket.socket) -> dict[str, Any]:
    """Terima dan uraikan satu pesan lengkap dari stream socket.

    Args:
        sock: Socket TCP yang sudah terhubung.

    Returns:
        Dict hasil parsing payload JSON.

    Raises:
        ValueError: Payload bukan JSON object yang valid.
        ConnectionError: Koneksi terputus di tengah pesan.
        socket.timeout: Melebiwati timeout socket.
        OSError: Kegagalan lain di level socket.
    """
    header = _recv_exactly(sock, HEADER_SIZE)
    (length,) = struct.unpack(HEADER_FORMAT, header)
    body = _recv_exactly(sock, length)
    try:
        message = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"payload bukan JSON UTF-8 valid: {exc}") from exc
    if not isinstance(message, dict):
        raise ValueError("payload JSON harus berupa object/dict, bukan array/scalar")
    return message


def validate_message_type(message: dict[str, Any]) -> str:
    """Ambil ``"type"`` pesan dan pastikan termasuk jenis yang dikenal.

    Args:
        message: Dict pesan hasil :func:`receive_message`.

    Returns:
        String tipe pesan.

    Raises:
        ValueError: Bila kunci ``"type"`` hilang atau tidak dikenal —
            penanganan pesan rusak menjadi jelas, bukan *silent fail*.
    """
    message_type = message.get("type")
    if message_type not in MESSAGE_TYPES:
        raise ValueError(f"pesan tanpa tipe valid; diterima: {message_type!r}")
    return str(message_type)

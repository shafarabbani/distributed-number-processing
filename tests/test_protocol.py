"""Unit test untuk dnp.protocol (pakai socket.socketpair, tanpa jaringan asli)."""

from __future__ import annotations

import pytest

from dnp.protocol import (
    receive_message,
    send_message,
    validate_message_type,
)


@pytest.fixture
def socket_pair():
    """Sepasang socket TCP-like terkoneksi dua arah."""
    import socket

    a, b = socket.socketpair()
    try:
        yield a, b
    finally:
        a.close()
        b.close()


class TestRoundTrip:
    def test_small_message(self, socket_pair) -> None:
        client, server = socket_pair
        payload = {"type": "ping", "value": 123}
        send_message(client, payload)
        assert receive_message(server) == payload

    def test_multiple_sequential_messages(self, socket_pair) -> None:
        client, server = socket_pair
        messages = [{"type": "ping", "seq": i} for i in range(5)]
        for message in messages:
            send_message(client, message)
        for message in messages:
            assert receive_message(server) == message

    def test_large_message_over_64kb_stays_intact(self, socket_pair) -> None:
        client, server = socket_pair
        numbers = list(range(50_000))
        payload = {"type": "data_chunk", "data": numbers}
        send_message(client, payload)
        received = receive_message(server)
        assert received["data"] == numbers

    def test_unicode_payload(self, socket_pair) -> None:
        client, server = socket_pair
        payload = {"type": "pong", "note": "angka-angkat ✓"}
        send_message(client, payload)
        assert receive_message(server) == payload


class TestErrorHandling:
    def test_non_dict_payload_rejected(self, socket_pair) -> None:
        client, _ = socket_pair
        with pytest.raises(ValueError):
            send_message(client, [1, 2, 3])  # type: ignore[arg-type]

    def test_non_serializable_payload_rejected(self, socket_pair) -> None:
        client, _ = socket_pair
        with pytest.raises(TypeError):
            send_message(client, {"obj": object()})

    def test_truncated_stream_raises_connection_error(self, socket_pair) -> None:
        import struct

        client, server = socket_pair
        client.sendall(struct.pack("!I", 100))  # janji 100 byte…
        client.close()  # …lalu koneksi ditutup -> pesan tidak pernah lengkap
        server.settimeout(2)
        with pytest.raises(ConnectionError):
            receive_message(server)

    def test_json_array_payload_rejected(self, socket_pair) -> None:
        import struct

        client, server = socket_pair
        body = b'[1, 2, 3]'
        client.sendall(struct.pack("!I", len(body)) + body)
        with pytest.raises(ValueError):
            receive_message(server)


class TestValidateMessageType:
    def test_known_types_pass(self) -> None:
        for message_type in ("data_chunk", "process_result", "ping", "pong"):
            assert validate_message_type({"type": message_type}) == message_type

    def test_unknown_type_raises(self) -> None:
        with pytest.raises(ValueError):
            validate_message_type({"type": "hack_attempt"})

    def test_missing_type_raises(self) -> None:
        with pytest.raises(ValueError):
            validate_message_type({"data": [1, 2]})

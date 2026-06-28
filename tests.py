"""
tests.py
--------
Unit and integration tests for the LAN Chat Application.

Run with:
    python -m pytest tests.py -v
    # or without pytest:
    python tests.py

Coverage:
- Protocol encoding / decoding
- FramedSocketReader fragmentation handling
- ClientRegistry concurrency
- Server integration (real sockets, loopback)
- Whisper routing
- Username validation rules
- Server-full rejection
"""

import socket
import threading
import time
import unittest

from protocol import (
    MessageType,
    FramedSocketReader,
    decode,
    encode,
)
from server import ChatServer, ClientRegistry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_server(port: int, max_clients: int = 10) -> ChatServer:
    srv = ChatServer("127.0.0.1", port, max_clients)
    t = threading.Thread(target=srv.start, daemon=True)
    t.start()
    time.sleep(0.05)   # let accept() start
    return srv


def _connect(port: int) -> socket.socket:
    sock = socket.create_connection(("127.0.0.1", port), timeout=3)
    sock.settimeout(3)
    return sock


def _send(sock: socket.socket, msg_type: MessageType, sender: str, payload: str) -> None:
    sock.sendall(encode(msg_type, sender, payload))


def _recv_one(sock: socket.socket) -> dict:
    """Read one framed message from socket."""
    reader = FramedSocketReader()
    while True:
        chunk = sock.recv(4096)
        for raw in reader.feed(chunk):
            return decode(raw)


def _join(sock: socket.socket, username: str) -> dict:
    _send(sock, MessageType.JOIN, username, username)
    return _recv_one(sock)


# ---------------------------------------------------------------------------
# Protocol tests
# ---------------------------------------------------------------------------

class TestProtocolEncodeDecode(unittest.TestCase):

    def test_roundtrip_text(self):
        raw = encode(MessageType.TEXT, "Alice", "Hello world")
        env = decode(raw.rstrip(b"\n"))
        self.assertEqual(env["type"],    MessageType.TEXT)
        self.assertEqual(env["sender"],  "Alice")
        self.assertEqual(env["payload"], "Hello world")

    def test_roundtrip_whisper(self):
        raw = encode(MessageType.WHISPER, "Bob", "Carol::secret")
        env = decode(raw.rstrip(b"\n"))
        self.assertEqual(env["payload"], "Carol::secret")

    def test_decode_missing_field_raises(self):
        import json
        bad = json.dumps({"type": "TEXT", "sender": "x"}).encode() + b"\n"
        with self.assertRaises(ValueError):
            decode(bad.rstrip(b"\n"))

    def test_decode_non_json_raises(self):
        with self.assertRaises(ValueError):
            decode(b"not json at all")

    def test_timestamp_present(self):
        raw = encode(MessageType.SYSTEM, "SERVER", "test")
        env = decode(raw.rstrip(b"\n"))
        self.assertIn("ts", env)
        self.assertIsInstance(env["ts"], float)


class TestFramedSocketReader(unittest.TestCase):
    """Simulate TCP fragmentation."""

    def _make_frames(self, *payloads) -> bytes:
        return b"".join(encode(MessageType.TEXT, "u", p) for p in payloads)

    def test_single_complete_message(self):
        reader = FramedSocketReader()
        data = encode(MessageType.TEXT, "Alice", "hi")
        msgs = reader.feed(data)
        self.assertEqual(len(msgs), 1)

    def test_multiple_in_one_chunk(self):
        reader = FramedSocketReader()
        data = self._make_frames("a", "b", "c")
        msgs = reader.feed(data)
        self.assertEqual(len(msgs), 3)

    def test_fragmented_across_chunks(self):
        reader = FramedSocketReader()
        full = self._make_frames("hello")
        mid  = len(full) // 2
        msgs = reader.feed(full[:mid])
        self.assertEqual(msgs, [])         # incomplete
        msgs = reader.feed(full[mid:])
        self.assertEqual(len(msgs), 1)

    def test_byte_by_byte(self):
        reader = FramedSocketReader()
        full = encode(MessageType.TEXT, "x", "byte-by-byte test")
        collected = []
        for byte in (bytes([b]) for b in full):
            collected.extend(reader.feed(byte))
        self.assertEqual(len(collected), 1)
        env = decode(collected[0])
        self.assertEqual(env["payload"], "byte-by-byte test")

    def test_two_and_half_messages(self):
        """2.5 messages in first chunk, rest in second."""
        reader = FramedSocketReader()
        full = self._make_frames("m1", "m2", "m3")
        split = len(self._make_frames("m1", "m2")) + 3   # halfway through m3
        msgs  = reader.feed(full[:split])
        self.assertEqual(len(msgs), 2)
        msgs += reader.feed(full[split:])
        self.assertEqual(len(msgs), 3)


# ---------------------------------------------------------------------------
# Registry tests
# ---------------------------------------------------------------------------

class TestClientRegistry(unittest.TestCase):

    def _make_dummy_handler(self):
        """Minimal stand-in — no real socket needed."""
        class Dummy:
            received = []
            def send_raw(self, data):
                self.received.append(data)
            def close(self):
                pass
        return Dummy()

    def test_register_unique(self):
        reg = ClientRegistry()
        h = self._make_dummy_handler()
        self.assertTrue(reg.register("Alice", h))
        self.assertFalse(reg.register("Alice", h))   # duplicate

    def test_unregister(self):
        reg = ClientRegistry()
        h = self._make_dummy_handler()
        reg.register("Bob", h)
        reg.unregister("Bob")
        self.assertNotIn("Bob", reg.usernames())

    def test_broadcast_excludes_sender(self):
        reg = ClientRegistry()
        alice = self._make_dummy_handler()
        bob   = self._make_dummy_handler()
        reg.register("Alice", alice)
        reg.register("Bob",   bob)
        msg = encode(MessageType.BROADCAST, "Alice", "hi")
        reg.broadcast(msg, exclude="Alice")
        self.assertEqual(len(alice.received), 0)
        self.assertEqual(len(bob.received),   1)

    def test_whisper_to_existing(self):
        reg = ClientRegistry()
        carol = self._make_dummy_handler()
        reg.register("Carol", carol)
        self.assertTrue(reg.whisper("Carol", b"secret"))
        self.assertEqual(len(carol.received), 1)

    def test_whisper_to_missing(self):
        reg = ClientRegistry()
        self.assertFalse(reg.whisper("Nobody", b"data"))

    def test_count(self):
        reg = ClientRegistry()
        for name in ("A", "B", "C"):
            reg.register(name, self._make_dummy_handler())
        self.assertEqual(reg.count(), 3)
        reg.unregister("B")
        self.assertEqual(reg.count(), 2)

    def test_concurrent_register(self):
        """100 threads race to register unique names — none should be lost."""
        reg = ClientRegistry()
        results = []
        lock = threading.Lock()

        def worker(name):
            ok = reg.register(name, self._make_dummy_handler())
            with lock:
                results.append(ok)

        threads = [threading.Thread(target=worker, args=(f"user{i}",))
                   for i in range(100)]
        for t in threads: t.start()
        for t in threads: t.join()

        self.assertEqual(sum(results), 100)   # all unique names succeed
        self.assertEqual(reg.count(), 100)


# ---------------------------------------------------------------------------
# Integration tests  (real loopback sockets)
# ---------------------------------------------------------------------------

BASE_PORT = 19000   # start here; each test uses the next free port

class TestServerIntegration(unittest.TestCase):
    _port_counter = BASE_PORT

    @classmethod
    def _next_port(cls) -> int:
        cls._port_counter += 1
        return cls._port_counter

    # -- helpers --

    def setUp(self):
        self.port = self._next_port()
        self.server = _make_server(self.port)

    def _client(self) -> socket.socket:
        return _connect(self.port)

    # -- tests --

    def test_successful_join(self):
        c = self._client()
        env = _join(c, "Alice")
        self.assertEqual(env["type"], MessageType.ACK)
        self.assertEqual(env["payload"], "Alice")
        c.close()

    def test_duplicate_username_rejected(self):
        c1 = self._client()
        _join(c1, "Bob")

        c2 = self._client()
        env = _join(c2, "Bob")
        self.assertEqual(env["type"], MessageType.REJECT)
        self.assertIn("taken", env["payload"].lower())
        c1.close(); c2.close()

    def test_empty_username_rejected(self):
        c = self._client()
        env = _join(c, "")
        self.assertEqual(env["type"], MessageType.REJECT)
        c.close()

    def test_username_too_long_rejected(self):
        c = self._client()
        env = _join(c, "A" * 21)
        self.assertEqual(env["type"], MessageType.REJECT)
        c.close()

    def test_broadcast_reaches_others(self):
        c1 = self._client(); _join(c1, "Sender")
        c2 = self._client(); _join(c2, "Receiver")
        # Flush the SYSTEM join notice for Receiver that Sender gets
        c1.settimeout(0.3)
        try:    _recv_one(c1)   # "Receiver joined"
        except: pass
        c1.settimeout(3)

        _send(c1, MessageType.TEXT, "Sender", "Hello everyone")

        # Both sender and receiver should get BROADCAST
        env = _recv_one(c2)
        self.assertEqual(env["type"], MessageType.BROADCAST)
        self.assertEqual(env["payload"], "Hello everyone")
        c1.close(); c2.close()

    def test_whisper_only_reaches_target(self):
        c1 = self._client(); _join(c1, "Whisperer")
        c2 = self._client(); _join(c2, "Target")
        c3 = self._client(); _join(c3, "Bystander")

        # Drain join notices
        for sock in (c1, c2, c3):
            sock.settimeout(0.2)
            for _ in range(3):
                try: _recv_one(sock)
                except: pass
            sock.settimeout(3)

        _send(c1, MessageType.WHISPER, "Whisperer", "Target::psst")

        env = _recv_one(c2)
        self.assertEqual(env["type"], MessageType.PRIVATE)
        self.assertIn("psst", env["payload"])

        # Bystander should NOT receive it
        c3.settimeout(0.3)
        with self.assertRaises(socket.timeout):
            _recv_one(c3)

        c1.close(); c2.close(); c3.close()

    def test_whisper_to_unknown_user(self):
        c = self._client(); _join(c, "Lonely")
        _send(c, MessageType.WHISPER, "Lonely", "Ghost::hello?")
        env = _recv_one(c)
        self.assertEqual(env["type"], MessageType.ERROR)
        self.assertIn("not found", env["payload"].lower())
        c.close()

    def test_list_users(self):
        c1 = self._client(); _join(c1, "UserA")
        c2 = self._client(); _join(c2, "UserB")
        # Drain join notices
        c1.settimeout(0.3)
        try: _recv_one(c1)
        except: pass
        c1.settimeout(3)

        _send(c1, MessageType.LIST, "UserA", "")
        env = _recv_one(c1)
        self.assertEqual(env["type"], MessageType.USERS)
        users = [u.strip() for u in env["payload"].split(",")]
        self.assertIn("UserA", users)
        self.assertIn("UserB", users)
        c1.close(); c2.close()

    def test_graceful_leave_notifies_others(self):
        c1 = self._client(); _join(c1, "Leaver")
        c2 = self._client(); _join(c2, "Watcher")

        # Drain the join notice Watcher gets for Leaver joining
        c2.settimeout(0.5)
        # The server broadcasts "Leaver joined" to Watcher
        notice_got = False
        for _ in range(3):
            try:
                env = _recv_one(c2)
                if env["type"] == MessageType.SYSTEM and "joined" in env["payload"]:
                    notice_got = True
                    break
            except socket.timeout:
                break
        c2.settimeout(3)

        _send(c1, MessageType.LEAVE, "Leaver", "")
        c1.close()
        time.sleep(0.2)

        env = _recv_one(c2)
        self.assertEqual(env["type"], MessageType.SYSTEM)
        self.assertIn("left", env["payload"].lower())
        c2.close()

    def test_server_full_rejection(self):
        """Server with max 1 client should reject a second connection."""
        port = self._next_port()
        _make_server(port, max_clients=1)

        c1 = _connect(port); _join(c1, "First")
        c2 = _connect(port)
        env = _join(c2, "Second")
        self.assertEqual(env["type"], MessageType.REJECT)
        self.assertIn("full", env["payload"].lower())
        c1.close(); c2.close()


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    unittest.main(verbosity=2)

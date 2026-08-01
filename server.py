"""
server.py
---------
Multithreaded LAN Chat Server.

Architecture
------------
- One main thread runs socket.accept() in a loop.
- Each connected client gets its own ClientHandler thread.
- A shared, thread-safe ClientRegistry tracks active connections.
- All broadcast / whisper routing goes through the registry.
- Graceful shutdown via SIGINT (Ctrl-C) closes all connections cleanly.

Usage
-----
    python server.py [--host 0.0.0.0] [--port 9000] [--max-clients 50]
"""

import argparse
import logging
import signal
import socket
import sys
import threading
import time
from typing import Optional

from protocol import (
    MessageType,
    FramedSocketReader,
    decode,
    encode,
    MAX_MESSAGE_BYTES,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("server")


# ---------------------------------------------------------------------------
# Client registry  (shared state — all access must hold _lock)
# ---------------------------------------------------------------------------

class ClientRegistry:
    """Thread-safe map of username → ClientHandler."""

    def __init__(self):
        self._clients: dict[str, "ClientHandler"] = {}
        self._lock = threading.Lock()

    # -- registration --

    def register(self, username: str, handler: "ClientHandler") -> bool:
        """Return True if username was free and is now registered."""
        with self._lock:
            if username in self._clients:
                return False
            self._clients[username] = handler
            return True

    def unregister(self, username: str) -> None:
        with self._lock:
            self._clients.pop(username, None)

    # -- routing --

    def broadcast(self, msg_bytes: bytes, exclude: Optional[str] = None) -> None:
        """Send msg_bytes to every client except `exclude`."""
        with self._lock:
            targets = [
                h for name, h in self._clients.items() if name != exclude
            ]
        for handler in targets:
            handler.send_raw(msg_bytes)

    def whisper(self, target: str, msg_bytes: bytes) -> bool:
        """Send privately to `target`. Returns False if target not found."""
        with self._lock:
            handler = self._clients.get(target)
        if handler is None:
            return False
        handler.send_raw(msg_bytes)
        return True

    def usernames(self) -> list[str]:
        with self._lock:
            return list(self._clients.keys())

    def count(self) -> int:
        with self._lock:
            return len(self._clients)

    def shutdown_all(self) -> None:
        with self._lock:
            handlers = list(self._clients.values())
        for h in handlers:
            h.close()


# ---------------------------------------------------------------------------
# Per-client handler thread
# ---------------------------------------------------------------------------

class ClientHandler(threading.Thread):
    """
    One instance per connected client.
    Runs in its own daemon thread; dies when the client disconnects.
    """

    def __init__(self, conn: socket.socket, addr: tuple, registry: ClientRegistry):
        super().__init__(daemon=True)
        self._conn     = conn
        self._addr     = addr
        self._registry = registry
        self._username: Optional[str] = None
        self._alive    = True
        self._send_lock = threading.Lock()   # serialize outgoing writes

    # -- public interface --

    def send_raw(self, data: bytes) -> None:
        """Thread-safe write. Silently drops on broken connection."""
        if not self._alive:
            return
        try:
            with self._send_lock:
                self._conn.sendall(data)
        except OSError:
            self._alive = False

    def close(self) -> None:
        self._alive = False
        try:
            self._conn.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self._conn.close()
        except OSError:
            pass

    # -- thread entry point --

    def run(self) -> None:
        log.info("New connection from %s:%d", *self._addr)
        reader = FramedSocketReader()

        try:
            # --- Step 1: wait for JOIN handshake (with timeout) ---
            self._conn.settimeout(10.0)
            if not self._handshake(reader):
                return

            self._conn.settimeout(None)   # blocking reads from here on

            # --- Step 2: main receive loop ---
            while self._alive:
                chunk = self._conn.recv(MAX_MESSAGE_BYTES)
                if not chunk:
                    break                 # client closed connection
                for raw in reader.feed(chunk):
                    self._handle(raw)

        except (OSError, ConnectionResetError):
            pass
        finally:
            self._cleanup()

    # -- private helpers --

    def _handshake(self, reader: FramedSocketReader) -> bool:
        """
        Read a JOIN message, validate username, register.
        Returns True if successful.
        """
        try:
            chunk = self._conn.recv(MAX_MESSAGE_BYTES)
        except socket.timeout:
            log.warning("%s:%d timed out during handshake", *self._addr)
            return False

        for raw in reader.feed(chunk):
            try:
                env = decode(raw)
            except ValueError as exc:
                log.warning("Bad handshake from %s:%d — %s", *self._addr, exc)
                self.send_raw(encode(MessageType.REJECT, "SERVER", str(exc)))
                return False

            if env["type"] != MessageType.JOIN:
                self.send_raw(encode(MessageType.REJECT, "SERVER", "Expected JOIN first"))
                return False

            username = env["payload"].strip()

            # Validate username
            if not username:
                self.send_raw(encode(MessageType.REJECT, "SERVER", "Username cannot be empty"))
                return False
            if len(username) > 20:
                self.send_raw(encode(MessageType.REJECT, "SERVER", "Username max 20 chars"))
                return False
            if not username.replace("_", "").replace("-", "").isalnum():
                self.send_raw(encode(MessageType.REJECT, "SERVER",
                                     "Username: letters, digits, - and _ only"))
                return False
            if username.upper() == "SERVER":
                self.send_raw(encode(MessageType.REJECT, "SERVER", "Reserved username"))
                return False

            if not self._registry.register(username, self):
                self.send_raw(encode(MessageType.REJECT, "SERVER",
                                     f"Username '{username}' is already taken"))
                return False

            self._username = username
            self.send_raw(encode(MessageType.ACK, "SERVER", username))

            # Announce arrival to others
            self._registry.broadcast(
                encode(MessageType.SYSTEM, "SERVER",
                       f"{username} joined the chat"),
                exclude=username,
            )
            log.info("'%s' registered from %s:%d  (total: %d)",
                     username, *self._addr, self._registry.count())
            return True

        return False    # no valid frame received

    def _handle(self, raw: bytes) -> None:
        """Dispatch a single decoded message from this client."""
        try:
            env = decode(raw)
        except ValueError as exc:
            self.send_raw(encode(MessageType.ERROR, "SERVER", str(exc)))
            return

        msg_type = env["type"]

        if msg_type == MessageType.TEXT:
            body = env["payload"].strip()
            if not body:
                return
            if len(body) > 500:
                self.send_raw(encode(MessageType.ERROR, "SERVER",
                                     "Message too long (max 500 chars)"))
                return
            out = encode(MessageType.BROADCAST, self._username, body)
            self._registry.broadcast(out)          # sender also gets it back

        elif msg_type == MessageType.WHISPER:
            # payload format: "target::body"
            if "::" not in env["payload"]:
                self.send_raw(encode(MessageType.ERROR, "SERVER",
                                     "Whisper format: /w <user> <message>"))
                return
            target, body = env["payload"].split("::", 1)
            target = target.strip()
            body   = body.strip()
            if target == self._username:
                self.send_raw(encode(MessageType.ERROR, "SERVER",
                                     "You cannot whisper to yourself"))
                return
            pm = encode(MessageType.PRIVATE, self._username,
                        f"(whisper) {body}")
            delivered = self._registry.whisper(target, pm)
            if delivered:
                # Echo back to sender so they see the whisper they sent
                self.send_raw(encode(MessageType.PRIVATE, "SERVER",
                                     f"[To {target}] {body}"))
            else:
                self.send_raw(encode(MessageType.ERROR, "SERVER",
                                     f"User '{target}' not found"))

        elif msg_type == MessageType.LIST:
            users = ", ".join(sorted(self._registry.usernames()))
            self.send_raw(encode(MessageType.USERS, "SERVER", users))

        elif msg_type == MessageType.LEAVE:
            self._alive = False   # triggers cleanup in run()

        else:
            self.send_raw(encode(MessageType.ERROR, "SERVER",
                                 f"Unexpected message type: {msg_type}"))

    def _cleanup(self) -> None:
        if self._username:
            self._registry.unregister(self._username)
            self._registry.broadcast(
                encode(MessageType.SYSTEM, "SERVER",
                       f"{self._username} left the chat")
            )
            log.info("'%s' disconnected  (total: %d)",
                     self._username, self._registry.count())
        self.close()


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------

class ChatServer:
    def __init__(self, host: str, port: int, max_clients: int):
        self.host        = host
        self.port        = port
        self.max_clients = max_clients
        self.registry    = ClientRegistry()
        self._server_sock: Optional[socket.socket] = None
        self._running    = False

    def start(self) -> None:
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.host, self.port))
        self._server_sock.listen(5)
        self._running = True

        log.info("LAN Chat Server listening on %s:%d  (max clients: %d)",
                 self.host, self.port, self.max_clients)
        log.info("Press Ctrl-C to stop")

        try:
            self._accept_loop()
        except KeyboardInterrupt:
            log.info("Shutdown signal received")
        finally:
            self._shutdown()

    def _accept_loop(self) -> None:
        while self._running:
            try:
                conn, addr = self._server_sock.accept()
            except OSError:
                break

            if self.registry.count() >= self.max_clients:
                # Immediately reject — server full
                try:
                    conn.sendall(encode(MessageType.REJECT, "SERVER",
                                        "Server is full"))
                    conn.close()
                except OSError:
                    pass
                log.warning("Rejected connection from %s:%d — server full", *addr)
                continue

            handler = ClientHandler(conn, addr, self.registry)
            handler.start()

    def _shutdown(self) -> None:
        self._running = False
        log.info("Closing all client connections …")
        self.registry.shutdown_all()
        try:
            self._server_sock.close()
        except OSError:
            pass
        log.info("Server stopped.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="LAN Chat Server")
    p.add_argument("--host",        default="0.0.0.0",   help="Bind address (default: 0.0.0.0)")
    p.add_argument("--port",        default=9000,  type=int, help="Port (default: 9000)")
    p.add_argument("--max-clients", default=50,   type=int, help="Max simultaneous clients")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    server = ChatServer(args.host, args.port, args.max_clients)
    server.start()

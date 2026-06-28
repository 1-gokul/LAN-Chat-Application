"""
client.py
---------
LAN Chat Client.

Architecture
------------
- Main thread handles user input (stdin).
- A dedicated ReceiverThread listens on the socket and prints incoming messages.
- Both share a single socket; sends are protected by a lock.
- Pure stdlib — no external dependencies required.

Usage
-----
    python client.py [--host 127.0.0.1] [--port 9000] [--username Alice]

Commands (type in chat)
-----------------------
    /w <user> <message>   — private message (whisper)
    /who                  — list online users
    /quit                 — leave gracefully
    /help                 — show commands
"""

import argparse
import logging
import socket
import sys
import threading
import time
from datetime import datetime
from typing import Optional

from protocol import (
    MessageType,
    FramedSocketReader,
    decode,
    encode,
    MAX_MESSAGE_BYTES,
)

# ---------------------------------------------------------------------------
# Logging  (client logs go to stderr so they don't pollute chat output)
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.WARNING,
    format="%(levelname)s %(message)s",
    stream=sys.stderr,
)
log = logging.getLogger("client")


# ---------------------------------------------------------------------------
# Colour helpers (ANSI, degrade gracefully on Windows without VT support)
# ---------------------------------------------------------------------------

import os

_USE_COLOR = sys.stdout.isatty() and os.name != "nt" or os.environ.get("TERM")

def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _USE_COLOR else text

def cyan(t):    return _c("96", t)
def yellow(t):  return _c("93", t)
def green(t):   return _c("92", t)
def red(t):     return _c("91", t)
def grey(t):    return _c("90", t)
def bold(t):    return _c("1",  t)
def magenta(t): return _c("95", t)


def ts() -> str:
    return grey(datetime.now().strftime("%H:%M:%S"))


# ---------------------------------------------------------------------------
# Receiver thread
# ---------------------------------------------------------------------------

class ReceiverThread(threading.Thread):
    """
    Reads from the socket in a loop; prints formatted messages to stdout.
    Sets `error_event` on connection loss so the main thread can exit.
    """

    def __init__(self, sock: socket.socket, send_lock: threading.Lock):
        super().__init__(daemon=True)
        self._sock      = sock
        self._send_lock = send_lock
        self.error_event = threading.Event()

    def run(self) -> None:
        reader = FramedSocketReader()
        try:
            while True:
                chunk = self._sock.recv(MAX_MESSAGE_BYTES)
                if not chunk:
                    print(f"\n{red('Connection closed by server.')}")
                    self.error_event.set()
                    return
                for raw in reader.feed(chunk):
                    self._render(raw)
        except OSError:
            if not self.error_event.is_set():
                print(f"\n{red('Lost connection to server.')}")
                self.error_event.set()

    def _render(self, raw: bytes) -> None:
        try:
            env = decode(raw)
        except ValueError:
            return

        t       = env["type"]
        sender  = env["sender"]
        payload = env["payload"]
        stamp   = ts()

        if t == MessageType.ACK:
            print(f"{stamp}  {green('✓ Joined as')} {bold(payload)}")
            print(f"       {grey('Type /help for commands')}")

        elif t == MessageType.REJECT:
            print(f"{stamp}  {red('✗ Rejected:')} {payload}")
            self.error_event.set()

        elif t == MessageType.BROADCAST:
            print(f"{stamp}  {cyan(sender):20s}  {payload}")

        elif t == MessageType.PRIVATE:
            print(f"{stamp}  {magenta(sender):20s}  {yellow(payload)}")

        elif t == MessageType.SYSTEM:
            print(f"{stamp}  {grey('*')} {grey(payload)}")

        elif t == MessageType.USERS:
            users = payload.split(",") if payload else []
            print(f"{stamp}  {green('Online')}: {', '.join(u.strip() for u in users)}")

        elif t == MessageType.ERROR:
            print(f"{stamp}  {red('Error')}: {payload}")

        else:
            print(f"{stamp}  [{t}] {payload}")


# ---------------------------------------------------------------------------
# Chat client
# ---------------------------------------------------------------------------

class ChatClient:
    HELP_TEXT = """
Commands:
  /w <user> <msg>   — send a private message
  /who              — list online users
  /quit             — leave the chat
  /help             — show this help
Any other text is sent as a public message.
"""

    def __init__(self, host: str, port: int, username: str):
        self.host     = host
        self.port     = port
        self.username = username
        self._sock: Optional[socket.socket] = None
        self._send_lock = threading.Lock()

    def connect(self) -> bool:
        try:
            self._sock = socket.create_connection((self.host, self.port), timeout=5)
            self._sock.settimeout(None)
        except (OSError, socket.timeout) as exc:
            print(red(f"Cannot connect to {self.host}:{self.port} — {exc}"))
            return False

        # Send JOIN
        self._send(MessageType.JOIN, self.username)

        # Wait briefly for ACK/REJECT before starting input loop
        self._sock.settimeout(5.0)
        try:
            chunk = self._sock.recv(MAX_MESSAGE_BYTES)
        except socket.timeout:
            print(red("Server did not respond to JOIN in time."))
            self._sock.close()
            return False
        self._sock.settimeout(None)

        reader = FramedSocketReader()
        for raw in reader.feed(chunk):
            try:
                env = decode(raw)
            except ValueError:
                continue
            if env["type"] == MessageType.REJECT:
                print(red(f"Rejected: {env['payload']}"))
                self._sock.close()
                return False
            if env["type"] == MessageType.ACK:
                self.username = env["payload"]   # server may normalise the name
                print(f"{ts()}  {green('✓ Connected to')} {bold(self.host)}:{self.port}"
                      f"  as  {bold(self.username)}")
                print(grey("Type /help for commands\n"))
                return True

        print(red("Unexpected response during handshake."))
        self._sock.close()
        return False

    def run(self) -> None:
        receiver = ReceiverThread(self._sock, self._send_lock)
        receiver.start()

        try:
            while not receiver.error_event.is_set():
                try:
                    line = input()
                except EOFError:
                    break

                if not line.strip():
                    continue

                self._dispatch(line.strip())
        except KeyboardInterrupt:
            pass
        finally:
            self._quit()

    # -- input dispatcher --

    def _dispatch(self, line: str) -> None:
        if line.startswith("/"):
            parts = line.split(None, 2)
            cmd   = parts[0].lower()

            if cmd == "/quit":
                self._quit()
                sys.exit(0)

            elif cmd == "/help":
                print(self.HELP_TEXT)

            elif cmd == "/who":
                self._send(MessageType.LIST, "")

            elif cmd == "/w":
                if len(parts) < 3:
                    print(red("Usage: /w <username> <message>"))
                    return
                target  = parts[1]
                body    = parts[2]
                self._send(MessageType.WHISPER, f"{target}::{body}")

            else:
                print(red(f"Unknown command '{cmd}'. Type /help."))
        else:
            self._send(MessageType.TEXT, line)

    # -- socket helpers --

    def _send(self, msg_type: MessageType, payload: str) -> None:
        if self._sock is None:
            return
        data = encode(msg_type, self.username, payload)
        try:
            with self._send_lock:
                self._sock.sendall(data)
        except OSError as exc:
            log.warning("Send failed: %s", exc)

    def _quit(self) -> None:
        try:
            self._send(MessageType.LEAVE, "")
            time.sleep(0.1)
            self._sock.close()
        except OSError:
            pass
        print(grey("\nDisconnected. Goodbye!"))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="LAN Chat Client")
    p.add_argument("--host",     default="127.0.0.1", help="Server IP (default: 127.0.0.1)")
    p.add_argument("--port",     default=9000,  type=int, help="Server port (default: 9000)")
    p.add_argument("--username", default="",    help="Your display name")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    username = args.username.strip()
    if not username:
        username = input("Enter your username: ").strip()
    if not username:
        print(red("Username cannot be empty."))
        sys.exit(1)

    client = ChatClient(args.host, args.port, username)
    if not client.connect():
        sys.exit(1)
    client.run()

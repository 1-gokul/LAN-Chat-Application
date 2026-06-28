"""
protocol.py
-----------
Defines the message protocol for LAN Chat.

Every message sent over the wire is a JSON-encoded dictionary followed by a
newline delimiter (\n).  Using JSON keeps the protocol human-readable and easy
to extend without breaking older clients.

Message envelope:
{
    "type"    : str,   # one of MessageType values
    "sender"  : str,   # username of the sender (empty for SERVER)
    "payload" : str,   # message body / metadata
    "ts"      : float  # unix timestamp (UTC)
}
"""

import json
import time
from enum import Enum


# ---------------------------------------------------------------------------
# Message types
# ---------------------------------------------------------------------------

class MessageType(str, Enum):
    # Client → Server
    JOIN    = "JOIN"     # client announces username
    LEAVE   = "LEAVE"    # client is disconnecting gracefully
    TEXT    = "TEXT"     # plain chat message
    WHISPER = "WHISPER"  # private message  payload = "target::body"
    LIST    = "LIST"     # request active user list

    # Server → Client
    ACK     = "ACK"      # join accepted; payload = assigned username
    REJECT  = "REJECT"   # join denied;   payload = reason
    BROADCAST = "BROADCAST"  # relayed TEXT to all clients
    PRIVATE = "PRIVATE"  # relayed WHISPER to target
    SYSTEM  = "SYSTEM"   # server announcements (join/leave notices)
    USERS   = "USERS"    # response to LIST; payload = comma-sep usernames
    ERROR   = "ERROR"    # generic server-side error


DELIMITER = b"\n"
MAX_MESSAGE_BYTES = 4096   # hard cap per message


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def encode(msg_type: MessageType, sender: str, payload: str) -> bytes:
    """Serialize a message to bytes ready to send over the socket."""
    envelope = {
        "type"   : msg_type.value,
        "sender" : sender,
        "payload": payload,
        "ts"     : time.time(),
    }
    return json.dumps(envelope).encode("utf-8") + DELIMITER


def decode(raw: bytes) -> dict:
    """
    Deserialize bytes → envelope dict.
    Raises ValueError on malformed input.
    """
    try:
        envelope = json.loads(raw.decode("utf-8").strip())
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"Malformed message: {exc}") from exc

    required = {"type", "sender", "payload", "ts"}
    if not required.issubset(envelope):
        raise ValueError(f"Missing fields: {required - envelope.keys()}")

    return envelope


class FramedSocketReader:
    """
    Stateful reader that buffers incoming bytes and yields complete messages.
    Handles TCP stream fragmentation — recv() does NOT guarantee full messages.

    Usage:
        reader = FramedSocketReader()
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            for msg_bytes in reader.feed(chunk):
                envelope = decode(msg_bytes)
                ...
    """

    def __init__(self):
        self._buf = b""

    def feed(self, chunk: bytes) -> list[bytes]:
        """Feed raw bytes; returns list of complete message byte-strings."""
        self._buf += chunk
        messages = []
        while DELIMITER in self._buf:
            line, self._buf = self._buf.split(DELIMITER, 1)
            if line:                        # skip blank lines
                messages.append(line)
        return messages

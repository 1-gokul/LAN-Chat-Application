# LAN Chat Application

A fully functional, multithreaded LAN chat server and client built in Python using raw TCP sockets — no third-party networking libraries.

---

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Protocol Design](#protocol-design)
- [Project Structure](#project-structure)
- [Setup & Usage](#setup--usage)
- [Commands](#commands)
- [Design Decisions](#design-decisions)
- [Concurrency Model](#concurrency-model)
- [Error Handling](#error-handling)
- [Running Tests](#running-tests)
- [Possible Extensions](#possible-extensions)

---

## Features

- **Real TCP socket programming** — no asyncio, no high-level networking libraries
- **Multithreaded server** — one thread per connected client
- **Thread-safe client registry** — concurrent broadcasts without data races
- **Custom application protocol** — JSON-framed messages over TCP with newline delimiters
- **TCP fragmentation handling** — `FramedSocketReader` buffers partial frames correctly
- **Public broadcast** — messages visible to all connected clients
- **Private whisper** — direct messages between two users
- **Graceful disconnect** — LEAVE message notifies other users
- **Username validation** — enforced server-side with clear rejection reasons
- **Server-full rejection** — configurable max client cap
- **27-test suite** — unit tests, concurrency tests, and full integration tests over loopback

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                     SERVER                          │
│                                                     │
│   MainThread                                        │
│   ─────────                                         │
│   socket.accept() loop                              │
│        │                                            │
│        ▼  (per connection)                          │
│   ClientHandler (Thread)   ←──────────────────┐    │
│   ─────────────────────                       │    │
│   recv loop → _handle()                       │    │
│        │                                      │    │
│        ▼                                      │    │
│   ClientRegistry (shared, thread-safe)        │    │
│   ───────────────────────────────────         │    │
│   { username → ClientHandler }                │    │
│        │                                      │    │
│        ├──── broadcast() → all handlers ──────┘    │
│        └──── whisper()   → target handler          │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────┐
│             CLIENT              │
│                                 │
│   MainThread                    │
│   ──────────                    │
│   stdin input → _dispatch()     │
│        │                        │
│        └──── sendall() ──────►  │──► TCP ──► Server
│                                 │
│   ReceiverThread (daemon)       │
│   ──────────────────────        │
│   recv() → _render()  ◄──────── │◄── TCP ◄── Server
└─────────────────────────────────┘
```

---

## Protocol Design

Every message on the wire is a **newline-delimited JSON object**:

```json
{
  "type"    : "TEXT",
  "sender"  : "Alice",
  "payload" : "Hello everyone",
  "ts"      : 1719564123.45
}
```

### Why newline-delimited JSON?

- **Human-readable** — easy to debug with `nc` or Wireshark
- **Self-describing** — no separate schema required
- **Extensible** — add new fields without breaking old clients
- **Simple framing** — newline as delimiter, no length-prefix parsing needed

### Message Types

| Direction       | Type        | Purpose                              |
|----------------|-------------|--------------------------------------|
| Client → Server | `JOIN`      | Register username                    |
| Client → Server | `TEXT`      | Public chat message                  |
| Client → Server | `WHISPER`   | Private message (`target::body`)     |
| Client → Server | `LIST`      | Request active user list             |
| Client → Server | `LEAVE`     | Graceful disconnect                  |
| Server → Client | `ACK`       | JOIN accepted                        |
| Server → Client | `REJECT`    | JOIN denied (reason in payload)      |
| Server → Client | `BROADCAST` | Relayed public message               |
| Server → Client | `PRIVATE`   | Relayed whisper                      |
| Server → Client | `SYSTEM`    | Server announcements (join/leave)    |
| Server → Client | `USERS`     | Response to LIST                     |
| Server → Client | `ERROR`     | Server-side error                    |

### Handshake Flow

```
Client                         Server
  │                               │
  │──── JOIN {username} ─────────►│
  │                               │  validate username
  │                               │  check for duplicates
  │◄─── ACK  {username} ──────────│  (or REJECT with reason)
  │                               │
  │     [chat begins]             │
  │                               │
  │──── LEAVE ───────────────────►│
  │◄─── [connection closed]       │
```

---

## Project Structure

```
lan-chat/
├── protocol.py    # Message types, encode/decode, FramedSocketReader
├── server.py      # ChatServer, ClientRegistry, ClientHandler
├── client.py      # ChatClient, ReceiverThread
├── tests.py       # 27 unit + integration tests
├── requirements.txt
└── README.md
```

---

## Setup & Usage

### Requirements

- Python 3.10+ (uses `match`-style type hints; `list[bytes]` syntax)
- No external packages required for core functionality
- `pytest` optional for test runner

```bash
pip install -r requirements.txt   # only needed for pytest
```

### Start the server

```bash
# Default: listen on all interfaces, port 9000, max 50 clients
python server.py

# Custom options
python server.py --host 192.168.1.10 --port 9000 --max-clients 100
```

### Connect a client

```bash
# On the same machine
python client.py --username Alice

# On another machine on the same LAN
python client.py --host 192.168.1.10 --port 9000 --username Bob
```

### Finding the server IP (LAN)

```bash
# Linux / macOS
ip addr show   # or: ifconfig

# Windows
ipconfig
```

---

## Commands

Once connected, type in the terminal:

| Command                  | Action                             |
|-------------------------|------------------------------------|
| `Hello everyone!`       | Send public message to all users   |
| `/w Alice Hey there`    | Send private message to Alice      |
| `/who`                  | List all online users              |
| `/help`                 | Show command reference             |
| `/quit`                 | Leave gracefully                   |
| `Ctrl-C`                | Force quit                         |

---

## Design Decisions

### Thread-per-client vs. async

This project deliberately uses **threads**, not `asyncio`. Reasons:

1. **Demonstrates OS-level concurrency** — threads, locks, race conditions — which is what networking interviews test
2. **Simpler to reason about** in a blocking I/O context
3. **Scales fine for LAN chat** — 50-100 clients is well within thread limits

For a production system handling thousands of concurrent connections, `asyncio` or an event loop would be the right call.

### Why a send lock per client?

`socket.sendall()` is **not thread-safe**. The server's broadcast path calls `send_raw()` from the sending client's thread, while the receiver thread may also be doing housekeeping. A per-client lock prevents interleaved writes that would corrupt the JSON framing.

### FramedSocketReader

TCP is a **stream protocol** — `recv()` returns *some* bytes, not necessarily a complete message. `FramedSocketReader` maintains a buffer across calls and yields only complete newline-terminated frames. This is a common and important interview topic.

### Username validation server-side

Validation lives in `server.py`, not `client.py`. The client is untrusted — it can be replaced by `nc` or a custom script. The server is the authority.

---

## Concurrency Model

```
Thread                Shared Resource        Protection
──────────────────    ──────────────────     ──────────────────────
ClientHandler[n]  →   ClientRegistry._clients   ClientRegistry._lock
ClientHandler[n]  →   ClientHandler._conn        ClientHandler._send_lock
MainThread        →   server socket              single writer (main only)
ReceiverThread    →   socket (reads only)        no lock needed (single reader)
```

Key invariant: **only one thread ever reads from a given client socket** (the ClientHandler for that client). Writes can come from any thread (broadcast), so `_send_lock` serialises them.

---

## Error Handling

| Scenario                        | Behaviour                                          |
|--------------------------------|----------------------------------------------------|
| Client crashes / network drop  | `recv()` returns empty bytes → cleanup runs        |
| Malformed JSON message         | `ValueError` caught → ERROR sent back to client   |
| Send to dead client            | `OSError` caught → `_alive = False`, thread exits |
| Handshake timeout (10s)        | Connection closed, logged as warning               |
| Server full                    | REJECT sent before ClientHandler is created        |
| Unknown command from client    | ERROR message sent, connection kept alive          |

---

## Running Tests

```bash
# With stdlib unittest (no install needed)
python tests.py

# With pytest
pip install pytest
pytest tests.py -v
```

### Test coverage

| Category              | Tests | What's covered                                        |
|-----------------------|-------|-------------------------------------------------------|
| Protocol              | 5     | encode/decode roundtrip, missing fields, bad JSON     |
| FramedSocketReader    | 5     | single, multiple, fragmented, byte-by-byte, 2.5 msgs  |
| ClientRegistry        | 7     | register, unregister, broadcast, whisper, concurrency |
| Server integration    | 10    | join, reject, broadcast, whisper, list, leave, full   |
| **Total**             | **27**| all passing                                           |

---

## Deployment

The application is deployed on **Railway**, providing simple cloud hosting and easy deployment.

## Possible Extensions

These are natural next steps to discuss in interviews or implement:

1. **Message history** — store last N messages in a deque; replay on join
2. **Chat rooms / channels** — partition the registry by room name
3. **Authentication** — password-based JOIN or token challenge-response
4. **TLS encryption** — wrap the socket with `ssl.wrap_socket()`
5. **Persistence** — write message log to SQLite
6. **GUI client** — replace terminal UI with Tkinter or a web frontend
7. **Rate limiting** — token bucket per client to prevent message flooding
8. **Async rewrite** — migrate to `asyncio` to support thousands of clients
9. **File transfer** — negotiate a side channel for binary payloads
10. **Admin commands** — kick, ban, mute via a privileged client role

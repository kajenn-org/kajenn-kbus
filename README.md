# kbus

[![Tests](https://github.com/kajenn-org/kajenn-kbus/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/kajenn-org/kajenn-kbus/actions/workflows/tests.yml)
[![Codecov](https://codecov.io/gh/kajenn-org/kajenn-kbus/branch/main/graph/badge.svg)](https://app.codecov.io/gh/kajenn-org/kajenn-kbus)
[![Documentation](https://readthedocs.org/projects/kajenn-kbus/badge/?version=latest)](https://kajenn-kbus.readthedocs.io/en/latest/)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](https://github.com/kajenn-org/kajenn-kbus/blob/main/pyproject.toml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue)](https://github.com/kajenn-org/kajenn-kbus/blob/main/LICENSE)

**Status**: Pre-Alpha · version 0.1.0.dev0, not yet on PyPI.

Move messages between parts of an application that may live in the same
process, in other processes on the same machine, or on other machines. The
calling code is the same in all three cases. Where a part runs is decided at
deployment, not in code.

A message is **metadata** (a small JSON object) plus a **payload** (`bytes`).
kbus reads the metadata to route the message. It never reads the payload.

Three layers, each built on the one below:

| Layer | What it is | Objects |
|---|---|---|
| `kbus.core` | two endpoints exchanging messages over one transport | `Connection`, `Message`, `Stream` |
| `kbus.dispatcher` | named members inside one application, routed by name | `Dispatcher`, `Member` |
| `kbus.link` | two applications, each trusting the other by token | `Link`, `LinkAcceptor` |

---

## Contents

1. [Core: messages, connections, streams](#core)
2. [Dispatcher: members and routes](#dispatcher)
3. [Link: between applications](#link)
4. [Failures](#failures)
5. [Limits and backpressure](#limits-and-backpressure)
6. [Transports and addresses](#transports-and-addresses)
7. [Test suite](#test-suite)
8. [What kbus does not do](#what-kbus-does-not-do)

---

## Core

### Message

```python
kbus.Message(meta={"op": "http", "user": "giovanni"}, payload=b"...")
```

- `meta`: a JSON object. Bounded in size. Readable by every process the
  message passes through.
- `payload`: `bytes`. Any content, any size up to the frame limit, including
  empty. kbus copies it, forwards it, never decodes it.

Keys that kbus writes in `meta` and that an application cannot set:
`id`, `kind`, `route`, `from`, `via`. Every other key belongs to the
application.

### Connection

One end of a transport. The other end is another `Connection`, in the same
process or elsewhere.

```python
a, b = kbus.core.pipe()                              # same process
conn = await kbus.core.connect("unix:///tmp/x.sock") # same machine
conn = await kbus.core.connect("wss://host/bus")     # other machine

async for conn in kbus.core.listen("unix:///tmp/x.sock"):
    ...
```

Each side installs one handler for what arrives:

```python
async def handler(message: kbus.Message, reply: kbus.Reply) -> None:
    ...

conn.handler = handler
```

### Three ways to send

**Call** — one message, one reply.

```python
reply = await conn.call(message)          # kbus.Message
```

The handler answers with `await reply.send(message)` or
`await reply.fail(exc, meta=None)`. `meta` travels with the error reply:
the caller finds it, plus `kind == "error"` and `error` (the exception class
name), in `.meta` of the exception it gets. A kbus error passed to `fail`
forwards its own `.meta` by default.

**Send** — one message, no reply.

```python
await conn.send(message)
```

`await` returns when the message is handed to the transport. It waits if the
transport's write buffer is full. Delivery is not confirmed. An error in the
handler on the other side is logged there and nothing comes back.

**Stream** — one call that opens a channel. Both sides send sequences of
messages on it.

```python
async with conn.open(message) as stream:
    await stream.send(kbus.Message(payload=chunk))   # my direction
    await stream.close()                             # I am done sending
    async for incoming in stream:                    # their direction
        ...
```

The handler receives the same `Stream` object as its second argument when the
incoming message has `kind == "open"`.

Stream rules:

- Messages arrive in the order they were sent. None is lost.
- A sender waits when the receiver is behind by more than the window
  (see [Limits](#limits-and-backpressure)).
- `close()` ends my direction. The other direction stays open.
- The stream is finished when both directions are closed.
- `abort(reason)` ends both directions at once. The other side gets
  `kbus.Aborted(reason)` from its next `send` or iteration.
- Leaving the `async with` block aborts the stream if it is not finished.
  This is the deterministic close. Iterating a stream outside `async with`
  and stopping early leaves it open until the object is collected.
- If the connection drops, both sides get `kbus.LinkLost`.

A call is a stream with one message in each direction. The transport carries
both the same way.

### Many calls at once

A connection carries any number of calls and streams at the same time. Each
reply reaches its own caller. Replies may come back in any order.

```python
r1, r2, r3 = await asyncio.gather(conn.call(m1), conn.call(m2), conn.call(m3))
```

### Both directions

Both ends of a connection have a handler and both can call. A handler may
call back on the same connection while it is answering.

### Timeout and cancellation

```python
async with asyncio.timeout(2):
    reply = await conn.call(message)
```

On timeout the caller's task is cancelled inside the block and `TimeoutError`
is raised outside it, as the standard library does. On cancellation of any
kind kbus sends a cancel to the other side: a handler still running gets
`asyncio.CancelledError`; a stream gets `kbus.Aborted`. kbus never defines a
subclass of `CancelledError`.

---

## Dispatcher

### Members and routes

A `Dispatcher` holds members. A `Member` has a name, a secret and a handler.
Names are unique while the member is connected.

```python
# the application process
dispatcher = kbus.Dispatcher(secrets={"billing": "b-secret", "shop": "s-secret"})
await dispatcher.listen("unix:///run/app.sock")
await dispatcher.listen("wss://0.0.0.0:8443/bus", ssl=ctx)
```

```python
# a member, any process
async def handler(message, reply): ...
billing = kbus.Member("billing", secret="b-secret", handler=handler)
await billing.connect("unix:///run/app.sock")   # or connect(dispatcher) in-process
```

A **route** is a dotted path. The first segment is a member name. The rest is
passed to that member in `meta["route"]`.

```python
reply = await shop.call("billing.total", kbus.Message(payload=b'{"order": 4}'))
```

The dispatcher reads `route`, writes `from` (the verified name of the sender),
and forwards the message to `billing`. It does not decode the payload.

A member that does not exist → `kbus.NoSuchMember`, immediately.

### Identity

`from` is written by the dispatcher from the name the member proved at
`connect` with its secret. A member cannot set or change it. Any key the
application adds to `meta` is application context, not identity. A policy
that decides who may do what reads `from`.

### Nested dispatchers

A member may hold a dispatcher for members below it. A route whose next
segment names one of those members is forwarded without touching the payload.
Other routes go to the member's own handler.

```python
group = kbus.Member("alfa", secret=..., handler=group_handler,
                    dispatcher=kbus.Dispatcher(secrets=worker_secrets))
await group.connect("wss://commander:8443/bus")
await group.dispatcher.listen("unix:///run/alfa.sock")
```

- `alfa.alfa_0003.http` → forwarded to member `alfa_0003` of `group.dispatcher`
- `alfa.assign` → `group_handler`

Replies travel back along the same path. `meta["via"]` lists the members the
message passed through.

### Hooks

Hooks see metadata only. They cannot change it or the payload.

```python
@dispatcher.on_message
async def policy(meta: dict) -> None:
    if meta["route"].startswith("admin.") and meta["from"] != "root":
        raise kbus.Refused("admin only")       # caller gets kbus.Refused

@group.on_reply
async def observe(meta: dict) -> None:
    ...   # every reply passing through this member, error replies
    ...   # included (meta["kind"] == "error"), before it goes on

@dispatcher.on_disconnect
async def gone(name: str, reason: Exception | None) -> None:
    ...
```

### Objects as members

For members whose handler is a Python object, a convenience on top of raw
messages. Only marked methods are reachable. Arguments and results are JSON.

```python
class Billing:
    @kbus.expose
    async def total(self, order: int) -> float: ...

billing = kbus.Member("billing", secret=..., handler=kbus.expose(Billing()))

total = await shop.route("billing").total(order=4)
```

Unmarked attributes → `kbus.NoSuchRoute`. Exceptions in the method →
`kbus.RemoteError` with `.type`, `.message`, `.traceback` (text), `.meta`.

---

## Link

Two applications, each with its own dispatcher, each trusting the other by a
token. One connection, opened by one side, carrying traffic in both
directions.

```python
# on the laptop, behind NAT
link = kbus.Link("gporcari-mac", token=my_token, url="wss://sourcerer.example/link")
dispatcher.attach_link("sourcerer", link)      # routes "sourcerer:..." go here
```

```python
# on the server
acceptor = kbus.LinkAcceptor(tokens={my_token: "gporcari-mac", ...})
await acceptor.listen("wss://0.0.0.0:443/link")
dispatcher.attach_acceptor(acceptor)           # each accepted link becomes "<name>:"
```

Routes across a link carry the instance name before `:`:

```python
await member.call("sourcerer:kb.ask", message)          # laptop → server
await member.call("gporcari-mac:claude.notify", message) # server → laptop
```

Rules:

- `name:route`. A route without `:` is local.
- Unknown instance name → `kbus.NoSuchInstance`, immediately.
- One hop only: a route arriving over a link may not name an instance
  itself; the receiving dispatcher refuses it with `kbus.Refused`.
- One link per token at a time. A second link presenting the token of a
  live link is refused and retries with its backoff; it enters once the
  first link drops.
- The link reconnects by itself after a drop, with backoff. `link.reachable`
  says whether it is up now. While it is down, calls to that instance fail
  immediately with `kbus.Unreachable`. Nothing is queued.
- On arrival, `from` is the instance name proved by the token. The members of
  the other application are not visible as members here.
- What the other application does with a message is decided by its own
  dispatcher and policies. The link carries application context in `meta`
  (for example a user credential); it never carries a resolved identity other
  than the instance name.

---

## Failures

| Situation | What the caller gets |
|---|---|
| Handler raised, or `reply.fail(exc)` | `kbus.RemoteError`, `.meta` from the error reply |
| No such member / route / instance | `kbus.NoSuchMember` / `kbus.NoSuchRoute` / `kbus.NoSuchInstance` |
| Refused by a policy, or an `instance:` route arriving over a link | `kbus.Refused` |
| Other side disconnected or died while the call was pending | `kbus.LinkLost` |
| Own connection dropped | `kbus.LinkLost` on every pending call and stream |
| Instance link down | `kbus.Unreachable` |
| Frame over the limit, sending | `kbus.FrameTooLarge`, nothing sent |
| Frame over the limit, receiving | connection closed, `kbus.ProtocolError` |
| Too many pending calls | `kbus.Overloaded`, nothing sent |
| Wrong secret or token, name already in use | `kbus.Rejected` from `connect` |

**`LinkLost` is an uncertain outcome.** The message may have been handled
before the link dropped. kbus does not retry and does not know. If a call
must not run twice, the application makes it idempotent or checks.

A member that dies in the middle of a stream: both sides of the stream get
`LinkLost`; an HTTP response built on it is truncated and the client
connection closed.

---

## Limits and backpressure

Every limit is a parameter. They are grouped in one object:

```python
limits = kbus.Limits(
    max_frame=16 * 2**20,      # metadata + payload, bytes
    max_meta=64 * 2**10,       # metadata, bytes
    max_route=4 * 2**10,       # route, bytes
    max_pending=1024,          # calls in flight per connection
    stream_window=32,          # messages in flight per stream
    write_buffer=4 * 2**20,    # transport write buffer, bytes
)
```

`Limits` is accepted by `Dispatcher`, `Member`, `Link`, `LinkAcceptor`,
`kbus.core.connect` and `kbus.core.listen`. A `Member` connected in-process
without its own `Limits` uses the dispatcher's. The values above are the
defaults.

A limit applies to the side that sets it: a sender does not send what exceeds
its own limits, a receiver rejects what exceeds its own. When the two sides
differ, the stricter one wins, with an explicit error on the side that
detects it.

Link reconnection has its own parameters, on `Link`:

```python
kbus.Link(..., reconnect=kbus.Reconnect(first=1.0, max=30.0, factor=2.0))
```

Behaviour at a limit:

- Over `max_frame`, `max_meta` or `max_route`: explicit error, never
  truncation.
- Over `max_pending`: `Overloaded` to the caller, nothing sent.
- Stream window full: `stream.send` waits for the receiver. Memory per
  stream is bounded by `stream_window × max_frame`.
- Write buffer full: `send`, `call` and `stream.send` wait. A slow peer slows
  its sender; it is never disconnected for being slow.

Data larger than one frame goes through a stream, one message per piece.

An application that embeds kbus exposes these parameters in its own
configuration. kajenn does so with an optional `kbus:` section of the server
grammar, one key per field of `Limits`, and a `link:` section for `Reconnect`.

## Transports and addresses

| Placement | Address | Transport |
|---|---|---|
| Same process | the `Dispatcher` object, or `kbus.core.pipe()` | in-memory queues; the payload object is passed, not copied |
| Same machine | `unix:///path/to.sock` | Unix domain socket, length-prefixed frames |
| Other machine | `ws://host:port/path`, `wss://...` | WebSocket, one frame per binary message, TLS with `wss` |

Every test in the suite runs on all three. Behaviour is the same; only
latency differs.

---

## Test suite

`tests/` is the specification. It uses the public API only: no imports of
private modules, no inspection of internal state.

- `tests/core/` — messages, calls, sends, streams, cancellation, limits,
  failures. Parametrised on the three transports.
- `tests/dispatcher/` — names, secrets, routes, nesting, hooks, `from`,
  disconnect of a member with pending calls.
- `tests/link/` — token handshake, `name:` routes, both directions on one
  connection, drop and reconnect, `Unreachable`.
- `tests/scenarios/` — a minimal HTTP gateway written in the tests, driven by
  a real HTTP and WebSocket client, with the handler in another process:
  every HTTP method, bodies of three sizes, a chunked response the client
  closes halfway, a WebSocket session closed from either side, the handler
  killed mid-response.

---

## What kbus does not do

- Spawn or supervise processes.
- Discover addresses. Addresses are configuration.
- Queue, persist or retry. What cannot be delivered now fails now.
- Broadcast. Every message has one destination.
- Serialise Python objects. `@kbus.expose` uses JSON; anything else is bytes.
- Decide trust beyond names: who may call what is a policy you write.

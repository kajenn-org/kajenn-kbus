"""A minimal ASGI gateway: HTTP and WebSocket in, kbus routes out.

The mapping is the one of app.py, read from the gateway's side:

- a request whose path does not start with ``/stream/`` is one call to
  ``app.http``: meta ``{"method", "path", "query", "headers"}``, the body as
  payload; the reply carries ``{"status", "headers"}`` and the body.
- a request under ``/stream/`` opens ``app.stream`` with the same meta. The
  gateway closes its own direction at once, sends the response head from the
  first incoming message and one chunk per following message, and aborts the
  stream with ``"client closed"`` if the client goes away first.
- a WebSocket session opens ``app.ws`` with ``{"path"}``; client frames go up
  as ``{"type": "text" | "bytes"}`` messages, app messages come down the same
  way; the client closing closes the gateway's direction, the app closing its
  direction closes the socket with 1000, the app vanishing closes it with 1011.

``NoSuchMember`` answers 503 (HTTP) or close 1013 (WebSocket). ``LinkLost``
before the head answers 502; after the head the gateway raises, so the server
drops the connection and the client sees a truncated response.
"""

import asyncio

import kbus


async def respond(send, status, body, content_type=b"application/json"):
    await send({"type": "http.response.start", "status": status,
                "headers": [[b"content-type", content_type]]})
    await send({"type": "http.response.body", "body": body})


async def wait_disconnect(receive):
    while True:
        event = await receive()
        if event["type"] == "http.disconnect":
            return


class Gateway:
    def __init__(self, member, app_name="app"):
        self.member = member
        self.app_name = app_name

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            await self.http(scope, receive, send)
        elif scope["type"] == "websocket":
            await self.websocket(scope, receive, send)
        else:
            raise RuntimeError(f"unsupported scope {scope['type']}")

    async def http(self, scope, receive, send):
        body = b""
        while True:
            event = await receive()
            if event["type"] == "http.disconnect":
                return
            body += event.get("body", b"")
            if not event.get("more_body"):
                break
        meta = {
            "method": scope["method"],
            "path": scope["path"],
            "query": scope["query_string"].decode(),
            "headers": [[k.decode(), v.decode()] for k, v in scope["headers"]],
        }
        if scope["path"].startswith("/stream/"):
            await self.streamed(meta, body, receive, send)
            return
        try:
            reply = await self.member.call(f"{self.app_name}.http",
                                           kbus.Message(meta=meta, payload=body))
        except kbus.NoSuchMember:
            await respond(send, 503, b'{"error": "unavailable"}')
            return
        except kbus.LinkLost:
            await respond(send, 502, b'{"error": "lost"}')
            return
        await send({"type": "http.response.start", "status": reply.meta["status"],
                    "headers": [[k.encode(), v.encode()] for k, v in reply.meta["headers"]]})
        await send({"type": "http.response.body", "body": reply.payload})

    async def streamed(self, meta, body, receive, send):
        try:
            async with self.member.open(f"{self.app_name}.stream",
                                        kbus.Message(meta=meta, payload=body)) as stream:
                await stream.close()
                try:
                    head = await anext(aiter(stream))
                except kbus.LinkLost:
                    await respond(send, 502, b'{"error": "lost"}')
                    return
                await send({"type": "http.response.start", "status": head.meta["status"],
                            "headers": [[k.encode(), v.encode()]
                                        for k, v in head.meta["headers"]]})

                async def forward():
                    async for chunk in stream:
                        await send({"type": "http.response.body", "body": chunk.payload,
                                    "more_body": True})

                forwarding = asyncio.create_task(forward())
                disconnect = asyncio.create_task(wait_disconnect(receive))
                done, _ = await asyncio.wait({forwarding, disconnect},
                                             return_when=asyncio.FIRST_COMPLETED)
                if disconnect in done and forwarding not in done:
                    forwarding.cancel()
                    await stream.abort("client closed")
                    return
                disconnect.cancel()
                forwarding.result()
                await send({"type": "http.response.body", "body": b"", "more_body": False})
        except kbus.NoSuchMember:
            await respond(send, 503, b'{"error": "unavailable"}')

    async def websocket(self, scope, receive, send):
        event = await receive()
        if event["type"] != "websocket.connect":
            return
        try:
            async with self.member.open(f"{self.app_name}.ws",
                                        kbus.Message(meta={"path": scope["path"]})) as stream:
                await send({"type": "websocket.accept"})
                client_open = True

                async def to_app():
                    nonlocal client_open
                    while True:
                        event = await receive()
                        if event["type"] == "websocket.disconnect":
                            client_open = False
                            if not stream.finished:
                                await stream.close()
                            return
                        if event.get("text") is not None:
                            message = kbus.Message(meta={"type": "text"},
                                                   payload=event["text"].encode())
                        else:
                            message = kbus.Message(meta={"type": "bytes"}, payload=event["bytes"])
                        try:
                            await stream.send(message)
                        except (kbus.LinkLost, kbus.Aborted):
                            return

                async def to_client():
                    code = 1000
                    try:
                        async for incoming in stream:
                            if incoming.meta["type"] == "text":
                                await send({"type": "websocket.send",
                                            "text": incoming.payload.decode()})
                            else:
                                await send({"type": "websocket.send", "bytes": incoming.payload})
                    except (kbus.LinkLost, kbus.Aborted):
                        code = 1011
                    if client_open:
                        await send({"type": "websocket.close", "code": code})

                await asyncio.gather(to_app(), to_client())
        except kbus.NoSuchMember:
            await send({"type": "websocket.close", "code": 1013})

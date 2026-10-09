"""The application side of the scenario suite: one kbus Member named ``app``.

Runs in-process (``App().handler`` given to a Member) or as its own process
(``python app.py`` with KBUS_ADDRESS, KBUS_NAME and KBUS_SECRET in the
environment). It imports kbus and nothing else from the project.

Routes it answers (the remainder after the member name):

- ``http``   — one call, one reply. ``/echo`` answers 200 with a JSON body
  describing the request; any other path answers 404.
- ``stream`` — one stream. First message: ``{"status", "headers"}``. Then one
  message per body chunk, then close. ``/stream/sse?n=<N>`` sends N events
  20 ms apart (default 50); ``/stream/slow`` sends one event every 100 ms
  until aborted or killed. A client that goes away is seen as ``Aborted``
  and counted.
- ``ws``     — one stream per WebSocket session. Each incoming message carries
  ``meta["type"]`` in ``{"text", "bytes"}``; the app echoes it with the
  payload prefixed by ``echo:``. The text ``bye`` makes the app close its
  direction. The client closing is counted.
- ``stats``  — one call, JSON reply with the counters above.
"""

import asyncio
import hashlib
import json
import os
import sys
from urllib.parse import parse_qs

import kbus


class App:
    def __init__(self):
        self.aborted = 0
        self.ws_closed_by_client = 0

    async def handler(self, message, second):
        route = message.meta["route"]
        if route == "http":
            await self.http(message, second)
        elif route == "stream":
            await self.stream(message, second)
        elif route == "ws":
            await self.ws(message, second)
        elif route == "stats":
            payload = json.dumps({"aborted": self.aborted,
                                  "ws_closed_by_client": self.ws_closed_by_client}).encode()
            await second.send(kbus.Message(payload=payload))
        else:
            await second.fail(LookupError(route))

    async def http(self, message, reply):
        meta = message.meta
        if meta["path"] != "/echo":
            await reply.send(kbus.Message(
                meta={"status": 404, "headers": [["content-type", "text/plain"]]},
                payload=b"not found"))
            return
        headers = dict(meta["headers"])
        body = json.dumps({
            "method": meta["method"],
            "path": meta["path"],
            "query": meta["query"],
            "body_len": len(message.payload),
            "sha256": hashlib.sha256(message.payload).hexdigest(),
            "x-test": headers.get("x-test"),
        }).encode()
        await reply.send(kbus.Message(
            meta={"status": 200, "headers": [["content-type", "application/json"],
                                             ["x-app", "yes"]]},
            payload=body))

    async def stream(self, message, stream):
        path = message.meta["path"]
        query = parse_qs(message.meta["query"])
        if path == "/stream/sse":
            count = int(query.get("n", ["50"])[0])
            interval = 0.02
        elif path == "/stream/slow":
            count = 10**9
            interval = 0.1
        else:
            await stream.send(kbus.Message(
                meta={"status": 404, "headers": [["content-type", "text/plain"]]}))
            await stream.close()
            return
        try:
            await stream.send(kbus.Message(
                meta={"status": 200, "headers": [["content-type", "text/event-stream"]]}))
            for i in range(count):
                await stream.send(kbus.Message(payload=f"data: {i}\n\n".encode()))
                await asyncio.sleep(interval)
            await stream.close()
        except kbus.Aborted:
            self.aborted += 1

    async def ws(self, message, stream):
        try:
            async for incoming in stream:
                if incoming.meta["type"] == "text" and incoming.payload == b"bye":
                    await stream.close()
                    return
                await stream.send(kbus.Message(meta={"type": incoming.meta["type"]},
                                               payload=b"echo:" + incoming.payload))
            self.ws_closed_by_client += 1
            await stream.close()
        except kbus.Aborted:
            pass


async def main():
    app = App()
    member = kbus.Member(os.environ["KBUS_NAME"], secret=os.environ["KBUS_SECRET"],
                         handler=app.handler)
    await member.connect(os.environ["KBUS_ADDRESS"])
    sys.stdout.write("ready\n")
    sys.stdout.flush()
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())

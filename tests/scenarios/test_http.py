import asyncio
import hashlib

import httpx
import pytest

from .conftest import wait_stats


@pytest.mark.parametrize("method", ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"])
async def test_every_method_whole_response(http_server, running_app, client, method):
    body = b"payload" if method in ("POST", "PUT", "PATCH") else None
    response = await client.request(method, f"http://{http_server}/echo", content=body)
    assert response.status_code == 200
    assert response.headers["x-app"] == "yes"
    if method == "HEAD":
        assert response.content == b""
        return
    echoed = response.json()
    assert echoed["method"] == method
    assert echoed["body_len"] == (len(body) if body else 0)


@pytest.mark.parametrize("size", [0, 100, 5 * 2**20])
async def test_bodies_of_three_sizes(http_server, running_app, client, size):
    body = bytes(range(256)) * (size // 256) + b"x" * (size % 256)
    response = await client.post(f"http://{http_server}/echo", content=body)
    assert response.status_code == 200
    echoed = response.json()
    assert echoed["body_len"] == size
    assert echoed["sha256"] == hashlib.sha256(body).hexdigest()


async def test_query_and_headers_reach_the_app(http_server, running_app, client):
    response = await client.get(f"http://{http_server}/echo?x=1&y=2", headers={"x-test": "t"})
    echoed = response.json()
    assert echoed["path"] == "/echo"
    assert echoed["query"] == "x=1&y=2"
    assert echoed["x-test"] == "t"


async def test_unknown_path_is_404(http_server, running_app, client):
    response = await client.get(f"http://{http_server}/nothing")
    assert response.status_code == 404
    assert response.content == b"not found"


async def test_streamed_response_arrives_whole(http_server, running_app, client):
    response = await client.get(f"http://{http_server}/stream/sse?n=5")
    assert response.status_code == 200
    assert response.headers["content-type"] == "text/event-stream"
    assert response.text == "".join(f"data: {i}\n\n" for i in range(5))


async def test_client_closing_halfway_aborts_the_stream(http_server, running_app, client,
                                                        gateway_member):
    async with client.stream("GET", f"http://{http_server}/stream/sse?n=500") as response:
        assert response.status_code == 200
        seen = 0
        async for _ in response.aiter_bytes():
            seen += 1
            if seen == 3:
                break
    await wait_stats(gateway_member, aborted=1)


async def test_app_absent_fails_at_once(http_server, client):
    async with asyncio.timeout(2):
        response = await client.get(f"http://{http_server}/echo")
    assert response.status_code == 503


async def test_app_dying_mid_response_truncates(http_server, running_app, client):
    if not running_app.can_die:
        pytest.skip("the in-process app cannot be killed")
    with pytest.raises(httpx.HTTPError):
        async with client.stream("GET", f"http://{http_server}/stream/slow") as response:
            assert response.status_code == 200
            chunks = response.aiter_bytes()
            await anext(chunks)
            await running_app.kill()
            async for _ in chunks:
                pass
    response = await client.get(f"http://{http_server}/echo")
    assert response.status_code == 503

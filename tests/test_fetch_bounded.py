import httpx
import pytest

from ontario_data.utils import DownloadTooLargeError, fetch_bounded


def _client(body: bytes, headers=None) -> httpx.AsyncClient:
    transport = httpx.MockTransport(lambda req: httpx.Response(200, content=body, headers=headers))
    return httpx.AsyncClient(transport=transport)


async def test_returns_body_under_cap():
    async with _client(b"a,b\n1,2\n") as c:
        assert await fetch_bounded(c, "https://x/f.csv", max_bytes=100) == b"a,b\n1,2\n"


async def test_rejects_body_over_cap():
    async with _client(b"x" * 1000) as c:
        with pytest.raises(DownloadTooLargeError):
            await fetch_bounded(c, "https://x/f.csv", max_bytes=100)

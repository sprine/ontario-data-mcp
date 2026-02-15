from unittest.mock import AsyncMock, patch

import httpx
import pytest

from ontario_data.ckan_client import CKANClient


def make_mock_transport(responses: list[httpx.Response]):
    """Create a mock transport that returns responses in order."""
    call_count = 0

    async def handle_request(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        idx = min(call_count, len(responses) - 1)
        call_count += 1
        return responses[idx]

    transport = httpx.MockTransport(handle_request)
    # Track call count on the transport object
    transport.call_count = lambda: call_count
    return transport


@pytest.mark.asyncio
@patch("asyncio.sleep", new_callable=AsyncMock)
async def test_retry_on_500(mock_sleep):
    """Should retry on 500 and succeed on next attempt."""
    transport = make_mock_transport([
        httpx.Response(500),
        httpx.Response(200, json={"success": True, "result": {"count": 1}}),
    ])
    async with httpx.AsyncClient(transport=transport) as http_client:
        client = CKANClient(http_client=http_client, base_delay=0.01)
        result = await client.package_search(query="test")
        assert result["count"] == 1
        assert mock_sleep.call_count >= 1


@pytest.mark.asyncio
@patch("asyncio.sleep", new_callable=AsyncMock)
async def test_retry_on_429(mock_sleep):
    """Should retry on 429 rate limit."""
    transport = make_mock_transport([
        httpx.Response(429),
        httpx.Response(200, json={"success": True, "result": ["tag1"]}),
    ])
    async with httpx.AsyncClient(transport=transport) as http_client:
        client = CKANClient(http_client=http_client, base_delay=0.01)
        result = await client.tag_list()
        assert result == ["tag1"]


@pytest.mark.asyncio
@patch("asyncio.sleep", new_callable=AsyncMock)
async def test_give_up_after_max_retries(mock_sleep):
    """Should raise after exhausting retries."""
    transport = make_mock_transport([
        httpx.Response(500),
        httpx.Response(500),
        httpx.Response(500),
        httpx.Response(500),
    ])
    async with httpx.AsyncClient(transport=transport) as http_client:
        client = CKANClient(http_client=http_client, max_retries=3, base_delay=0.01)
        with pytest.raises(httpx.HTTPStatusError):
            await client.package_search(query="test")


@pytest.mark.asyncio
async def test_no_retry_on_404():
    """Should not retry on 404 — it's not a retryable status."""
    transport = make_mock_transport([
        httpx.Response(404),
    ])
    async with httpx.AsyncClient(transport=transport) as http_client:
        client = CKANClient(http_client=http_client, base_delay=0.01)
        with pytest.raises(httpx.HTTPStatusError):
            await client.package_show("nonexistent")


@pytest.mark.asyncio
@patch("asyncio.sleep", new_callable=AsyncMock)
async def test_retry_on_connection_error(mock_sleep):
    """Should retry on connection errors."""
    call_count = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise httpx.ConnectError("Connection refused")
        return httpx.Response(200, json={"success": True, "result": {"id": "abc"}})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http_client:
        client = CKANClient(http_client=http_client, base_delay=0.01)
        result = await client.package_show("abc")
        assert result["id"] == "abc"
        assert mock_sleep.call_count >= 1


@pytest.mark.asyncio
async def test_successful_request_no_retry():
    """Should not retry on successful 200 response."""
    transport = make_mock_transport([
        httpx.Response(200, json={"success": True, "result": {"count": 5}}),
    ])
    async with httpx.AsyncClient(transport=transport) as http_client:
        client = CKANClient(http_client=http_client, base_delay=0.01)
        result = await client.package_search(query="test")
        assert result["count"] == 5

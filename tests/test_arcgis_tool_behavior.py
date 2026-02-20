"""Tests for tool behavior with ArcGIS Hub portals."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from ontario_data.portals import PORTALS


@pytest.fixture
def make_portal_context():
    """Factory fixture: create a mock MCP context with full portal state."""
    def _make(portal_clients=None):
        ctx = MagicMock()
        ctx.fastmcp._lifespan_result = {
            "cache": MagicMock(),
            "http_client": MagicMock(),
            "portal_configs": PORTALS,
            "portal_clients": portal_clients or {},
        }
        ctx.report_progress = AsyncMock()
        return ctx
    return _make


class TestSqlQueryArcGIS:
    @pytest.mark.asyncio
    async def test_returns_not_available(self, make_portal_context):
        from ontario_data.tools.querying import sql_query

        ottawa_client = AsyncMock()
        ctx = make_portal_context(portal_clients={"ottawa": ottawa_client})
        result = json.loads(await sql_query(sql="SELECT 1", portal="ottawa", ctx=ctx))

        assert result["status"] == "not_available"
        assert "download_resource" in result["suggestion"]
        ottawa_client.datastore_sql.assert_not_called()


class TestQueryResourceArcGIS:
    @pytest.mark.asyncio
    async def test_returns_not_available(self, make_portal_context):
        from ontario_data.tools.querying import query_resource

        ottawa_client = AsyncMock()
        ctx = make_portal_context(portal_clients={"ottawa": ottawa_client})
        result = json.loads(await query_resource(resource_id="ottawa:abc123_0", ctx=ctx))

        assert result["status"] == "not_available"
        assert "download_resource" in result["suggestion"]


class TestPreviewDataArcGIS:
    @pytest.mark.asyncio
    async def test_returns_not_available(self, make_portal_context):
        from ontario_data.tools.querying import preview_data

        ottawa_client = AsyncMock()
        ctx = make_portal_context(portal_clients={"ottawa": ottawa_client})
        result = json.loads(await preview_data(resource_id="ottawa:abc123_0", ctx=ctx))

        assert result["status"] == "not_available"
        assert "download_resource" in result["suggestion"]


class TestGetResourceSchemaArcGIS:
    @pytest.mark.asyncio
    async def test_returns_not_available(self, make_portal_context):
        from ontario_data.tools.metadata import get_resource_schema

        ottawa_client = AsyncMock()
        ctx = make_portal_context(portal_clients={"ottawa": ottawa_client})
        result = json.loads(await get_resource_schema(resource_id="ottawa:abc123_0", ctx=ctx))

        assert result["status"] == "not_available"
        assert "download_resource" in result["suggestion"]


class TestLoadGeodataArcGIS:
    @pytest.mark.asyncio
    async def test_returns_not_available(self, make_portal_context):
        from ontario_data.tools.geospatial import load_geodata

        ottawa_client = AsyncMock()
        ctx = make_portal_context(portal_clients={"ottawa": ottawa_client})
        result = json.loads(await load_geodata(resource_id="ottawa:abc123_0", ctx=ctx))

        assert result["status"] == "not_available"
        assert "download_resource" in result["suggestion"]

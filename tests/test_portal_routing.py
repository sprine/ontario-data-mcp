"""Tests for multi-portal routing, session context, and new portal tools."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from ontario_data.portals import PORTALS, PortalConfig, PortalType


def make_portal_context(active_portal="ontario", portal_clients=None):
    """Create a mock MCP context with full portal state."""
    ctx = MagicMock()
    ctx.fastmcp._lifespan_result = {
        "cache": MagicMock(),
        "http_client": MagicMock(),
        "portal_configs": PORTALS,
        "portal_clients": portal_clients or {},
        "active_portal": active_portal,
    }
    ctx.report_progress = AsyncMock()
    return ctx


class TestPortalRegistry:
    def test_has_three_portals(self):
        assert set(PORTALS.keys()) == {"ontario", "toronto", "ottawa"}

    def test_ontario_is_ckan(self):
        assert PORTALS["ontario"].portal_type == PortalType.CKAN

    def test_toronto_is_ckan(self):
        assert PORTALS["toronto"].portal_type == PortalType.CKAN

    def test_ottawa_is_arcgis(self):
        assert PORTALS["ottawa"].portal_type == PortalType.ARCGIS_HUB

    def test_portal_config_is_frozen(self):
        with pytest.raises(AttributeError):
            PORTALS["ontario"].name = "changed"

    def test_all_have_base_url(self):
        for key, config in PORTALS.items():
            assert config.base_url.startswith("https://"), f"{key} missing https URL"


class TestGetDeps:
    def test_default_uses_active_portal(self):
        from ontario_data.utils import get_deps

        mock_ckan = AsyncMock()
        ctx = make_portal_context(
            active_portal="ontario",
            portal_clients={"ontario": mock_ckan},
        )
        client, cache = get_deps(ctx)
        assert client is mock_ckan

    def test_explicit_portal_override(self):
        from ontario_data.utils import get_deps

        ontario_ckan = AsyncMock()
        toronto_ckan = AsyncMock()
        ctx = make_portal_context(
            active_portal="ontario",
            portal_clients={"ontario": ontario_ckan, "toronto": toronto_ckan},
        )
        client, cache = get_deps(ctx, portal="toronto")
        assert client is toronto_ckan

    def test_unknown_portal_raises(self):
        from ontario_data.utils import get_deps

        ctx = make_portal_context()
        with pytest.raises(ValueError, match="Unknown portal 'nonexistent'"):
            get_deps(ctx, portal="nonexistent")

    def test_lazy_client_creation(self):
        from ontario_data.utils import get_deps

        ctx = make_portal_context(portal_clients={})
        # Ontario is CKAN, should be lazily created
        client, cache = get_deps(ctx)
        state = ctx.fastmcp._lifespan_result
        assert "ontario" in state["portal_clients"]

    def test_arcgis_portal_raises_not_yet_supported(self):
        from ontario_data.utils import get_deps

        ctx = make_portal_context(portal_clients={})
        with pytest.raises(ValueError, match="not yet supported"):
            get_deps(ctx, portal="ottawa")


class TestSetPortal:
    @pytest.mark.asyncio
    async def test_set_valid_portal(self):
        from ontario_data.tools.discovery import set_portal

        ctx = make_portal_context(active_portal="ontario")
        result = json.loads(await set_portal.fn(portal="toronto", ctx=ctx))
        assert result["status"] == "ok"
        assert result["active_portal"] == "toronto"
        # Verify state was actually changed
        assert ctx.fastmcp._lifespan_result["active_portal"] == "toronto"

    @pytest.mark.asyncio
    async def test_set_invalid_portal(self):
        from ontario_data.tools.discovery import set_portal

        ctx = make_portal_context()
        result = json.loads(await set_portal.fn(portal="bogus", ctx=ctx))
        assert "error" in result
        assert "available_portals" in result
        # Active portal unchanged
        assert ctx.fastmcp._lifespan_result["active_portal"] == "ontario"


class TestListPortals:
    @pytest.mark.asyncio
    async def test_lists_all_portals(self):
        from ontario_data.tools.discovery import list_portals

        ctx = make_portal_context(active_portal="ontario")
        result = json.loads(await list_portals.fn(ctx=ctx))
        assert result["active_portal"] == "ontario"
        assert len(result["portals"]) == 3
        keys = [p["key"] for p in result["portals"]]
        assert "ontario" in keys
        assert "toronto" in keys
        assert "ottawa" in keys

    @pytest.mark.asyncio
    async def test_marks_active_portal(self):
        from ontario_data.tools.discovery import list_portals

        ctx = make_portal_context(active_portal="toronto")
        result = json.loads(await list_portals.fn(ctx=ctx))
        active = [p for p in result["portals"] if p["active"]]
        assert len(active) == 1
        assert active[0]["key"] == "toronto"


class TestSearchAllPortals:
    @pytest.mark.asyncio
    async def test_searches_ckan_portals_concurrently(self):
        from ontario_data.tools.discovery import search_all_portals

        ontario_ckan = AsyncMock()
        ontario_ckan.package_search.return_value = {
            "count": 1,
            "results": [{"id": "ds1", "title": "Ontario Transit", "resources": [], "organization": {"title": "MTO"}, "tags": []}],
        }
        toronto_ckan = AsyncMock()
        toronto_ckan.package_search.return_value = {
            "count": 2,
            "results": [{"id": "ds2", "title": "TTC Routes", "resources": [], "organization": {"title": "TTC"}, "tags": []}],
        }

        ctx = make_portal_context(
            portal_clients={"ontario": ontario_ckan, "toronto": toronto_ckan},
        )
        result = json.loads(await search_all_portals.fn(query="transit", ctx=ctx))
        assert result["portals_searched"] == 2
        assert result["portals_skipped"] == 1  # Ottawa (ArcGIS)
        assert len(result["results"]) == 2

        # Verify both portals were searched
        portal_names = [r["portal"] for r in result["results"]]
        assert "ontario" in portal_names
        assert "toronto" in portal_names

    @pytest.mark.asyncio
    async def test_skipped_portals_noted(self):
        from ontario_data.tools.discovery import search_all_portals

        ctx = make_portal_context(
            portal_clients={
                "ontario": AsyncMock(package_search=AsyncMock(return_value={"count": 0, "results": []})),
                "toronto": AsyncMock(package_search=AsyncMock(return_value={"count": 0, "results": []})),
            },
        )
        result = json.loads(await search_all_portals.fn(query="anything", ctx=ctx))
        assert len(result["skipped"]) == 1
        assert result["skipped"][0]["portal"] == "ottawa"

    @pytest.mark.asyncio
    async def test_handles_portal_error_gracefully(self):
        from ontario_data.tools.discovery import search_all_portals

        ontario_ckan = AsyncMock()
        ontario_ckan.package_search.return_value = {"count": 0, "results": []}
        toronto_ckan = AsyncMock()
        toronto_ckan.package_search.side_effect = Exception("Connection timeout")

        ctx = make_portal_context(
            portal_clients={"ontario": ontario_ckan, "toronto": toronto_ckan},
        )
        result = json.loads(await search_all_portals.fn(query="transit", ctx=ctx))
        # Should still return results, with error noted for Toronto
        assert result["portals_searched"] == 2
        toronto_result = next(r for r in result["results"] if r["portal"] == "toronto")
        assert "error" in toronto_result


class TestMakeTableNamePortalPrefix:
    def test_default_portal_is_ontario(self):
        from ontario_data.utils import make_table_name

        result = make_table_name("covid cases", "abcd1234")
        assert result.startswith("ds_ontario_")

    def test_toronto_prefix(self):
        from ontario_data.utils import make_table_name

        result = make_table_name("ttc routes", "abcd1234", portal="toronto")
        assert result.startswith("ds_toronto_")

    def test_different_portals_produce_different_names(self):
        from ontario_data.utils import make_table_name

        ont = make_table_name("transit", "abcd1234", portal="ontario")
        tor = make_table_name("transit", "abcd1234", portal="toronto")
        assert ont != tor


class TestBackwardCompatibility:
    """Verify default portal=ontario preserves existing behavior."""

    @pytest.mark.asyncio
    async def test_search_datasets_defaults_to_ontario(self):
        from ontario_data.tools.discovery import search_datasets

        mock_ckan = AsyncMock()
        mock_ckan.package_search.return_value = {"count": 0, "results": []}
        ctx = make_portal_context(
            active_portal="ontario",
            portal_clients={"ontario": mock_ckan},
        )
        await search_datasets.fn(query="test", ctx=ctx)
        mock_ckan.package_search.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_dataset_info_defaults_to_ontario(self):
        from ontario_data.tools.metadata import get_dataset_info

        mock_ckan = AsyncMock()
        mock_ckan.package_show.return_value = {
            "id": "test-id",
            "resources": [],
            "tags": [],
            "organization": {},
        }
        ctx = make_portal_context(
            active_portal="ontario",
            portal_clients={"ontario": mock_ckan},
        )
        await get_dataset_info.fn(dataset_id="test-ds", ctx=ctx)
        mock_ckan.package_show.assert_called_once_with("test-ds")

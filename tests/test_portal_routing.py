"""Tests for multi-portal routing: parse_portal_id, fan_out, search fan-out, get_deps."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from ontario_data.portals import PORTALS, PortalType


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
    def test_explicit_portal(self, make_portal_context):
        from ontario_data.utils import get_deps

        toronto_ckan = AsyncMock()
        ctx = make_portal_context(
            portal_clients={"toronto": toronto_ckan},
        )
        client, cache = get_deps(ctx, portal="toronto")
        assert client is toronto_ckan

    def test_unknown_portal_raises(self, make_portal_context):
        from ontario_data.utils import get_deps

        ctx = make_portal_context()
        with pytest.raises(ValueError, match="Unknown portal 'nonexistent'"):
            get_deps(ctx, portal="nonexistent")

    def test_lazy_client_creation(self, make_portal_context):
        from ontario_data.utils import get_deps

        ctx = make_portal_context(portal_clients={})
        client, cache = get_deps(ctx, portal="ontario")
        state = ctx.fastmcp._lifespan_result
        assert "ontario" in state["portal_clients"]

    def test_arcgis_portal_creates_client(self, make_portal_context):
        from ontario_data.arcgis_client import ArcGISHubClient
        from ontario_data.utils import get_deps

        ctx = make_portal_context(portal_clients={})
        client, cache = get_deps(ctx, portal="ottawa")
        assert isinstance(client, ArcGISHubClient)


class TestParsePortalId:
    def test_prefixed_id(self):
        from ontario_data.utils import parse_portal_id

        portal, bare = parse_portal_id("toronto:abc123", {"ontario", "toronto", "ottawa"})
        assert portal == "toronto"
        assert bare == "abc123"

    def test_bare_id(self):
        from ontario_data.utils import parse_portal_id

        portal, bare = parse_portal_id("abc123", {"ontario", "toronto", "ottawa"})
        assert portal is None
        assert bare == "abc123"

    def test_urn_not_treated_as_portal(self):
        from ontario_data.utils import parse_portal_id

        portal, bare = parse_portal_id("urn:uuid:abc", {"ontario", "toronto", "ottawa"})
        assert portal is None
        assert bare == "urn:uuid:abc"

    def test_unknown_prefix_is_bare(self):
        from ontario_data.utils import parse_portal_id

        portal, bare = parse_portal_id("bogus:abc", {"ontario", "toronto"})
        assert portal is None
        assert bare == "bogus:abc"


class TestFanOut:
    @pytest.mark.asyncio
    async def test_first_match_stops_early(self, make_portal_context):
        from ontario_data.utils import fan_out

        ontario_ckan = AsyncMock()
        ctx = make_portal_context(portal_clients={"ontario": ontario_ckan})

        call_log = []

        async def _fn(pk: str):
            call_log.append(pk)
            if pk == "ontario":
                return "found"
            raise ValueError("not found")

        results = await fan_out(ctx, None, _fn, first_match=True)
        assert len(results) == 1
        assert results[0] == ("ontario", "found", None)
        # Should stop after ontario succeeds — toronto never called
        assert call_log == ["ontario"]

    @pytest.mark.asyncio
    async def test_first_match_all_fail_returns_errors(self, make_portal_context):
        from ontario_data.utils import fan_out

        ctx = make_portal_context(portal_clients={})

        async def _fn(pk: str):
            raise ValueError(f"not found on {pk}")

        results = await fan_out(ctx, None, _fn, first_match=True)
        # Should have errors for all 3 portals
        assert len(results) == 3
        portals = [r[0] for r in results]
        assert "ontario" in portals
        assert "toronto" in portals
        assert "ottawa" in portals
        assert all(r[2] is not None for r in results)

    @pytest.mark.asyncio
    async def test_parallel_fan_out_collects_all(self, make_portal_context):
        from ontario_data.utils import fan_out

        ctx = make_portal_context(portal_clients={})

        async def _fn(pk: str):
            return f"result_{pk}"

        results = await fan_out(ctx, None, _fn)
        # All 3 portals
        assert len(results) == 3
        portals = {r[0] for r in results}
        assert portals == {"ontario", "toronto", "ottawa"}
        assert all(r[2] is None for r in results)

    @pytest.mark.asyncio
    async def test_portal_param_narrows_to_one(self, make_portal_context):
        from ontario_data.utils import fan_out

        ctx = make_portal_context(portal_clients={})

        async def _fn(pk: str):
            return f"result_{pk}"

        results = await fan_out(ctx, "toronto", _fn)
        assert len(results) == 1
        assert results[0][0] == "toronto"


class TestSearchDatasetsFanOut:
    @pytest.mark.asyncio
    async def test_fans_out_to_all_portals(self, make_portal_context):
        from ontario_data.tools.discovery import search_datasets

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
        ottawa_arcgis = AsyncMock()
        ottawa_arcgis.package_search.return_value = {
            "count": 1,
            "results": [{"id": "ds3_0", "title": "OC Transpo", "resources": [], "organization": {"title": "City of Ottawa"}, "tags": []}],
        }

        ctx = make_portal_context(
            portal_clients={"ontario": ontario_ckan, "toronto": toronto_ckan, "ottawa": ottawa_arcgis},
        )
        result = json.loads(await search_datasets(query="transit", ctx=ctx))
        assert result["portals_searched"] == 3
        assert len(result["results"]) == 3

        portal_names = [r["portal"] for r in result["results"]]
        assert "ontario" in portal_names
        assert "toronto" in portal_names
        assert "ottawa" in portal_names

    @pytest.mark.asyncio
    async def test_portal_param_narrows(self, make_portal_context):
        from ontario_data.tools.discovery import search_datasets

        toronto_ckan = AsyncMock()
        toronto_ckan.package_search.return_value = {
            "count": 1,
            "results": [{"id": "ds2", "title": "TTC Routes", "resources": [], "organization": {"title": "TTC"}, "tags": []}],
        }

        ctx = make_portal_context(portal_clients={"toronto": toronto_ckan})
        result = json.loads(await search_datasets(query="transit", portal="toronto", ctx=ctx))
        assert result["portals_searched"] == 1
        assert len(result["results"]) == 1
        assert result["results"][0]["portal"] == "toronto"

    @pytest.mark.asyncio
    async def test_error_graceful(self, make_portal_context):
        from ontario_data.tools.discovery import search_datasets

        ontario_ckan = AsyncMock()
        ontario_ckan.package_search.return_value = {"count": 0, "results": []}
        toronto_ckan = AsyncMock()
        toronto_ckan.package_search.side_effect = Exception("Connection timeout")

        ctx = make_portal_context(
            portal_clients={"ontario": ontario_ckan, "toronto": toronto_ckan},
        )
        result = json.loads(await search_datasets(query="transit", ctx=ctx))
        # Ontario results should still be returned
        assert result["portals_searched"] == 1
        # Toronto should be in skipped with error
        skipped_portals = {s["portal"] for s in result["skipped"]}
        assert "toronto" in skipped_portals

    @pytest.mark.asyncio
    async def test_prefixed_ids(self, make_portal_context):
        from ontario_data.tools.discovery import search_datasets

        ontario_ckan = AsyncMock()
        ontario_ckan.package_search.return_value = {
            "count": 1,
            "results": [{"id": "ds1", "title": "Test", "resources": [], "organization": {"title": "Org"}, "tags": []}],
        }

        ctx = make_portal_context(portal_clients={"ontario": ontario_ckan})
        result = json.loads(await search_datasets(query="test", portal="ontario", ctx=ctx))
        ds = result["results"][0]["datasets"][0]
        assert ds["id"] == "ontario:ds1"


class TestListPortals:
    @pytest.mark.asyncio
    async def test_lists_all_portals(self, make_portal_context):
        from ontario_data.tools.discovery import list_portals

        ctx = make_portal_context()
        result = json.loads(await list_portals(ctx=ctx))
        assert len(result["portals"]) == 3
        keys = [p["key"] for p in result["portals"]]
        assert "ontario" in keys
        assert "toronto" in keys
        assert "ottawa" in keys

    @pytest.mark.asyncio
    async def test_no_active_marker(self, make_portal_context):
        from ontario_data.tools.discovery import list_portals

        ctx = make_portal_context()
        result = json.loads(await list_portals(ctx=ctx))
        assert "active_portal" not in result
        for p in result["portals"]:
            assert "active" not in p


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

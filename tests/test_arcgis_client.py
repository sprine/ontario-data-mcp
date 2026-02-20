"""Tests for ArcGISHubClient — mocked HTTP responses."""
from __future__ import annotations

from unittest.mock import AsyncMock

import httpx
import pytest

from ontario_data.arcgis_client import ArcGISHubClient

_FAKE_REQUEST = httpx.Request("GET", "https://open.ottawa.ca/api/test")


def make_ogc_search_response(features: list[dict], total: int = 1) -> dict:
    """Build a minimal OGC Records API response."""
    return {
        "type": "FeatureCollection",
        "features": features,
        "numberMatched": total,
        "numberReturned": len(features),
    }


def make_ogc_feature(
    item_id: str = "abc123",
    layer_index: int = 0,
    title: str = "Test Dataset",
    description: str = "A test",
    tags: list[str] | None = None,
    modified: str = "2025-06-01T00:00:00Z",
) -> dict:
    return {
        "id": f"{item_id}_{layer_index}",
        "type": "Feature",
        "properties": {
            "id": f"{item_id}_{layer_index}",
            "title": title,
            "description": description,
            "tags": tags or ["transit"],
            "owner": "CityofOttawa_GISsupport",
            "modified": modified,
            "type": "Feature Service",
            "url": f"https://services.arcgis.com/G6F8XLCl5KtAlZ2G/arcgis/rest/services/{title}/FeatureServer/0",
        },
    }


def make_hub_v3_dataset(
    ds_id: str = "abc123_0",
    title: str = "Park Parking Lots",
    description: str = "City parks",
    tags: list[str] | None = None,
    modified: str = "2025-06-01T00:00:00Z",
    url: str = "https://services.arcgis.com/.../FeatureServer/0",
) -> dict:
    """Build a minimal Hub v3 dataset response."""
    return {
        "data": {
            "id": ds_id,
            "type": "dataset",
            "attributes": {
                "id": ds_id,
                "name": title.lower().replace(" ", "-"),
                "title": title,
                "description": description,
                "tags": tags or ["parks"],
                "url": url,
                "modified": modified,
                "created": "2020-01-01T00:00:00Z",
                "license": "Open Government Licence",
                "owner": "CityofOttawa_GISsupport",
                "orgName": "City of Ottawa",
                "layers": [
                    {"id": 0, "name": title, "geometryType": "esriGeometryPoint"}
                ],
            },
        }
    }


def make_downloads_api_response(csv_url: str = "https://open.ottawa.ca/datasets/abc123_0.csv") -> dict:
    return {
        "data": [
            {
                "id": "abc123_0_csv",
                "type": "download",
                "attributes": {
                    "format": "csv",
                    "url": csv_url,
                    "status": "ready",
                }
            }
        ]
    }


class TestPackageSearch:
    @pytest.mark.asyncio
    async def test_basic_search(self):
        mock_response = httpx.Response(
            200,
            request=_FAKE_REQUEST,
            json=make_ogc_search_response(
                [make_ogc_feature(title="Ottawa Cycling")], total=1
            ),
        )
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.get.return_value = mock_response

        client = ArcGISHubClient(
            base_url="https://open.ottawa.ca", http_client=mock_client
        )
        result = await client.package_search(query="cycling", rows=5)

        assert result["count"] == 1
        assert len(result["results"]) == 1
        ds = result["results"][0]
        assert ds["title"] == "Ottawa Cycling"
        assert ds["id"] == "abc123_0"
        assert ds["organization"]["title"] == "City of Ottawa"

    @pytest.mark.asyncio
    async def test_pagination_params(self):
        mock_response = httpx.Response(
            200,
            request=_FAKE_REQUEST,
            json=make_ogc_search_response([], total=0),
        )
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.get.return_value = mock_response

        client = ArcGISHubClient(
            base_url="https://open.ottawa.ca", http_client=mock_client
        )
        await client.package_search(query="test", rows=10, start=20)

        call_args = mock_client.get.call_args
        params = call_args.kwargs.get("params") or call_args[1].get("params", {})
        assert params["limit"] == 10
        assert params["startindex"] == 20

    @pytest.mark.asyncio
    async def test_empty_results(self):
        mock_response = httpx.Response(
            200,
            request=_FAKE_REQUEST,
            json=make_ogc_search_response([], total=0),
        )
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.get.return_value = mock_response

        client = ArcGISHubClient(
            base_url="https://open.ottawa.ca", http_client=mock_client
        )
        result = await client.package_search(query="nonexistent")
        assert result["count"] == 0
        assert result["results"] == []


class TestPackageShow:
    @pytest.mark.asyncio
    async def test_returns_ckan_shape(self):
        mock_response = httpx.Response(200, request=_FAKE_REQUEST, json=make_hub_v3_dataset())
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.get.return_value = mock_response

        client = ArcGISHubClient(
            base_url="https://open.ottawa.ca", http_client=mock_client
        )
        ds = await client.package_show("abc123_0")

        assert ds["id"] == "abc123_0"
        assert ds["title"] == "Park Parking Lots"
        assert ds["notes"] == "City parks"
        assert ds["organization"]["title"] == "City of Ottawa"
        assert isinstance(ds["tags"], list)
        assert ds["tags"][0]["name"] == "parks"
        assert isinstance(ds["resources"], list)
        assert len(ds["resources"]) >= 1

    @pytest.mark.asyncio
    async def test_404_raises(self):
        mock_response = httpx.Response(404, request=_FAKE_REQUEST, json={"error": "not found"})
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.get.return_value = mock_response

        client = ArcGISHubClient(
            base_url="https://open.ottawa.ca", http_client=mock_client
        )
        with pytest.raises(httpx.HTTPStatusError):
            await client.package_show("nonexistent_0")


class TestResourceShow:
    @pytest.mark.asyncio
    async def test_returns_resource_dict(self):
        """resource_show calls package_show and extracts the matching resource."""
        mock_response = httpx.Response(200, request=_FAKE_REQUEST, json=make_hub_v3_dataset(ds_id="abc123_0"))
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.get.return_value = mock_response

        client = ArcGISHubClient(
            base_url="https://open.ottawa.ca", http_client=mock_client
        )
        resource = await client.resource_show("abc123_0")
        assert resource["id"] == "abc123_0"
        assert resource["package_id"] == "abc123_0"
        assert "url" in resource
        assert resource["datastore_active"] is False


class TestCompatMethods:
    @pytest.mark.asyncio
    async def test_organization_list_returns_single_org(self):
        client = ArcGISHubClient(
            base_url="https://open.ottawa.ca",
            http_client=AsyncMock(),
            org_name="ottawa",
            org_title="City of Ottawa",
        )
        orgs = await client.organization_list()
        assert len(orgs) == 1
        assert orgs[0]["title"] == "City of Ottawa"
        assert "package_count" not in orgs[0]

    @pytest.mark.asyncio
    async def test_tag_list_returns_empty(self):
        client = ArcGISHubClient(base_url="https://open.ottawa.ca", http_client=AsyncMock())
        tags = await client.tag_list()
        assert tags == []


class TestDownloadMethods:
    @pytest.mark.asyncio
    async def test_get_download_url_csv(self):
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.get.return_value = httpx.Response(
            200, request=_FAKE_REQUEST, json=make_downloads_api_response("https://example.com/data.csv")
        )

        client = ArcGISHubClient(
            base_url="https://open.ottawa.ca", http_client=mock_client
        )
        url = await client.get_download_url("abc123_0", fmt="csv")
        assert url == "https://example.com/data.csv"

    @pytest.mark.asyncio
    async def test_get_download_url_not_found(self):
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.get.return_value = httpx.Response(404, request=_FAKE_REQUEST)

        client = ArcGISHubClient(
            base_url="https://open.ottawa.ca", http_client=mock_client
        )
        url = await client.get_download_url("abc123_0", fmt="csv")
        assert url is None

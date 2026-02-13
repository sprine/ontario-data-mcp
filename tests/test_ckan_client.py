import pytest
import httpx
import respx
from ontario_data.ckan_client import CKANClient

BASE_URL = "https://data.ontario.ca"


@pytest.fixture
def client():
    return CKANClient(base_url=BASE_URL)


class TestPackageSearch:
    @respx.mock
    @pytest.mark.asyncio
    async def test_basic_search(self, client):
        respx.get(f"{BASE_URL}/api/3/action/package_search").mock(
            return_value=httpx.Response(200, json={
                "success": True,
                "result": {
                    "count": 1,
                    "results": [{"id": "abc", "title": "Test Dataset", "name": "test-dataset"}],
                },
            })
        )
        result = await client.package_search(query="test")
        assert result["count"] == 1
        assert result["results"][0]["title"] == "Test Dataset"

    @respx.mock
    @pytest.mark.asyncio
    async def test_search_with_filters(self, client):
        route = respx.get(f"{BASE_URL}/api/3/action/package_search").mock(
            return_value=httpx.Response(200, json={
                "success": True,
                "result": {"count": 0, "results": []},
            })
        )
        await client.package_search(query="health", filters={"organization": "health"}, rows=5)
        request = route.calls[0].request
        assert "fq=organization%3Ahealth" in str(request.url) or "organization" in str(request.url)


class TestPackageShow:
    @respx.mock
    @pytest.mark.asyncio
    async def test_get_dataset(self, client):
        respx.get(f"{BASE_URL}/api/3/action/package_show").mock(
            return_value=httpx.Response(200, json={
                "success": True,
                "result": {
                    "id": "abc",
                    "title": "Test",
                    "resources": [{"id": "r1", "format": "CSV", "url": "http://example.com/data.csv"}],
                },
            })
        )
        result = await client.package_show("abc")
        assert result["title"] == "Test"
        assert len(result["resources"]) == 1


class TestDatastoreSearch:
    @respx.mock
    @pytest.mark.asyncio
    async def test_basic_datastore_query(self, client):
        respx.get(f"{BASE_URL}/api/3/action/datastore_search").mock(
            return_value=httpx.Response(200, json={
                "success": True,
                "result": {
                    "total": 100,
                    "records": [{"_id": 1, "name": "Alice"}],
                    "fields": [{"id": "_id", "type": "int"}, {"id": "name", "type": "text"}],
                },
            })
        )
        result = await client.datastore_search("r1", limit=1)
        assert result["total"] == 100
        assert result["records"][0]["name"] == "Alice"

    @respx.mock
    @pytest.mark.asyncio
    async def test_datastore_sql(self, client):
        respx.get(f"{BASE_URL}/api/3/action/datastore_search_sql").mock(
            return_value=httpx.Response(200, json={
                "success": True,
                "result": {
                    "records": [{"count": 42}],
                    "fields": [{"id": "count", "type": "int"}],
                },
            })
        )
        result = await client.datastore_sql('SELECT count(*) FROM "r1"')
        assert result["records"][0]["count"] == 42


class TestListEndpoints:
    @respx.mock
    @pytest.mark.asyncio
    async def test_tag_list(self, client):
        respx.get(f"{BASE_URL}/api/3/action/tag_list").mock(
            return_value=httpx.Response(200, json={
                "success": True,
                "result": ["economy", "health", "education"],
            })
        )
        result = await client.tag_list()
        assert "health" in result

    @respx.mock
    @pytest.mark.asyncio
    async def test_organization_list(self, client):
        respx.get(f"{BASE_URL}/api/3/action/organization_list").mock(
            return_value=httpx.Response(200, json={
                "success": True,
                "result": [{"name": "health", "title": "Health", "package_count": 386}],
            })
        )
        result = await client.organization_list(all_fields=True)
        assert result[0]["name"] == "health"


class TestErrorHandling:
    @respx.mock
    @pytest.mark.asyncio
    async def test_api_error_raises(self, client):
        respx.get(f"{BASE_URL}/api/3/action/package_show").mock(
            return_value=httpx.Response(200, json={
                "success": False,
                "error": {"message": "Not found", "__type": "Not Found Error"},
            })
        )
        with pytest.raises(Exception, match="Not found"):
            await client.package_show("nonexistent")

    @respx.mock
    @pytest.mark.asyncio
    async def test_http_error_raises(self, client):
        respx.get(f"{BASE_URL}/api/3/action/package_show").mock(
            return_value=httpx.Response(500)
        )
        with pytest.raises(httpx.HTTPStatusError):
            await client.package_show("anything")


class TestPagination:
    @respx.mock
    @pytest.mark.asyncio
    async def test_paginate_all_results(self, client):
        call_count = 0

        def handler(request):
            nonlocal call_count
            start = int(request.url.params.get("start", 0))
            if start == 0:
                call_count += 1
                return httpx.Response(200, json={
                    "success": True,
                    "result": {
                        "count": 3,
                        "results": [{"id": "a"}, {"id": "b"}],
                    },
                })
            else:
                call_count += 1
                return httpx.Response(200, json={
                    "success": True,
                    "result": {
                        "count": 3,
                        "results": [{"id": "c"}],
                    },
                })

        respx.get(f"{BASE_URL}/api/3/action/package_search").mock(side_effect=handler)
        results = await client.package_search_all(query="test", page_size=2)
        assert len(results) == 3
        assert call_count == 2

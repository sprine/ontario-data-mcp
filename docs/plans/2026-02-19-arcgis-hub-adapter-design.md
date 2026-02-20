# ArcGIS Hub Adapter Design

Date: 2026-02-19
Scope: Ottawa only — prove the pattern, add other ArcGIS portals later.

## Decisions

- **Duck typing, no Protocol.** `ArcGISHubClient` implements the same method signatures as `CKANClient`. `get_deps()` returns either client transparently. No `PortalClient` Protocol needed — only 2 portal types exist.
- **Download logic stays in `retrieval.py`.** Add `_download_arcgis_resource_data()` alongside the existing CKAN function. Branch on `portal_type` in the tool. Don't refactor working CKAN code.
- **Downloads API first, Feature Service fallback.** Bulk CSV via Downloads API is fast. Fall back to Feature Service pagination if the download endpoint isn't available.
- **Unsupported tools return informative messages.** No exceptions — return JSON with `status: "not_available"`, `reason`, and `suggestion`.
- **Ottawa only.** One portal end-to-end with smoke tests. Other ArcGIS portals added later as config-only changes.

## 1. ArcGISHubClient

New file: `src/ontario_data/arcgis_client.py`

Constructor takes `base_url` and `http_client` (same as `CKANClient`). Shared `httpx.AsyncClient` from lifespan.

### Methods matching CKANClient signatures

| Method | ArcGIS implementation |
|--------|----------------------|
| `package_search(query, filters, sort, rows, start)` | OGC Records API: `GET /api/search/v1/collections/all/items?q=...&limit=...&startindex=...`. Normalize response to `{"count": N, "results": [...]}` matching CKAN shape. |
| `package_show(id)` | Hub v3 API: `GET /api/v3/datasets/{id}`. Normalize to CKAN-like dict with `id`, `name`, `title`, `notes`, `organization`, `tags`, `resources`, `metadata_modified`. |
| `resource_show(id)` | Synthesized from `package_show`. ArcGIS "resource" = a layer or download format. Returns dict with `id`, `package_id`, `format`, `url`, `datastore_active` (False). |
| `datastore_search_all(resource_id)` | Feature Service pagination: `GET {service_url}/query?where=1=1&outFields=*&resultOffset=N&resultRecordCount=M&f=json`. Returns `{"records": [...], "fields": [...], "total": N}`. |
| `organization_list(...)` | Returns `[{"name": "ottawa", "title": "City of Ottawa", "package_count": ~665, "description": "Single-org portal"}]`. |
| `tag_list(...)` | Returns `[]`. ArcGIS Hub doesn't expose a tag listing endpoint. |
| `close()` | No-op (shared http_client closed by lifespan). |

### Methods that raise informative errors

`datastore_sql`, `datastore_search`, `resource_search`, `group_list`, `package_list` — raise `NotImplementedError` with a clear message and suggestion to use `download_resource` + `query_cached`.

### Response normalization

`package_search` must return results where each dataset dict has:
- `id` — the ArcGIS `{itemId}_{layerIndex}` format
- `name` — slug derived from title
- `title`, `notes` (description), `metadata_modified`
- `organization` — `{"title": "City of Ottawa", "name": "ottawa"}`
- `tags` — extracted from ArcGIS `tags` or `keyword` fields
- `resources` — synthesized list with format and URL info

## 2. Integration into get_deps and fan_out

### `get_deps()` in `utils.py`

Add `elif config.portal_type == PortalType.ARCGIS_HUB:` branch that creates `ArcGISHubClient`. Return type becomes `tuple[CKANClient | ArcGISHubClient, CacheManager]`.

### `fan_out()` in `utils.py`

Remove the CKAN-only filter. Change line 101 from:
```python
keys = [k for k, c in configs.items() if c.portal_type == PortalType.CKAN]
```
to:
```python
keys = list(configs.keys())
```

### `search_datasets` in `discovery.py`

Remove lines 91-93 (the "ArcGIS Hub support coming soon" skip logic). Ottawa participates in fan-out naturally.

## 3. Download path in retrieval.py

New function `_download_arcgis_resource_data(client, resource_id)` alongside existing `_download_resource_data`.

**Strategy:**
1. `client.package_show(dataset_id)` for metadata
2. Try Downloads API: `GET /api/v3/datasets/{id}/downloads?spatialRefId=4326&format=csv` → CSV URL → `pd.read_csv()`
3. If Downloads API fails (404 / no CSV), fall back to `client.datastore_search_all(resource_id)` → Feature Service pagination → DataFrame
4. Return `(df, resource_meta, dataset_meta)` — same tuple shape

**`download_resource` tool** branches on portal type:
```python
config = configs[portal]
if config.portal_type == PortalType.ARCGIS_HUB:
    df, resource, dataset = await _download_arcgis_resource_data(client, bare_id)
else:
    df, resource, dataset = await _download_resource_data(client, bare_id)
```

**`refresh_cache`** gets the same branch (already infers portal from table name prefix).

### Resource ID mapping

ArcGIS datasets use `{itemId}_{layerIndex}` (e.g. `abc123_0`). Single-layer datasets have layer index `0`. The `resource_id` for ArcGIS is `{itemId}_{layerIndex}`. Portal-prefixed form: `ottawa:abc123_0`.

## 4. Tools that don't apply to ArcGIS

| Tool | Behavior |
|------|----------|
| `sql_query` | Return `{"status": "not_available", "reason": "ArcGIS Hub has no remote SQL API", "suggestion": "Use download_resource + query_cached instead"}` |
| `query_resource` | Same as above |
| `get_resource_schema` | Return "download first, then use query_cached to inspect" (or synthesize from Feature Service `?f=json` if feasible) |

Fan-out tools (`list_organizations`, `list_topics`) include Ottawa. The client methods return informative results rather than errors.

Tools that work unchanged (operate on cached DuckDB data):
- `query_cached`, `cache_info`, `cache_manage`

Tools that work via duck-typed client methods:
- `search_datasets`, `get_popular_datasets`, `search_by_location`, `get_dataset_info`, `find_related_datasets`

## 5. Testing

### Unit tests (`tests/test_arcgis_client.py`)
- Mock HTTP for each ArcGIS endpoint (OGC Records, Hub v3, Downloads API, Feature Service)
- Verify response normalization matches CKAN shape
- Verify `organization_list()` and `tag_list()` return informative messages
- Verify unsupported methods raise clear errors

### Portal routing updates (`tests/test_portal_routing.py`)
- `get_deps(ctx, "ottawa")` returns `(ArcGISHubClient, CacheManager)`
- `fan_out` with `portal=None` includes Ottawa
- `search_datasets` results include Ottawa

### Download tests
- Mock Downloads API CSV path → DataFrame
- Mock Downloads API 404 → Feature Service fallback
- Verify same `(df, resource_meta, dataset_meta)` tuple shape

### Integration smoke test
- Hit real Ottawa ArcGIS Hub APIs (gated behind `--live` flag)
- Search for a known dataset, download, query cached

## Files to create/modify

| File | Action | Description |
|------|--------|-------------|
| `src/ontario_data/arcgis_client.py` | **CREATE** | ArcGIS Hub client with CKANClient-compatible method signatures |
| `src/ontario_data/utils.py` | Modify | `get_deps()` creates ArcGISHubClient; `fan_out()` includes all portal types |
| `src/ontario_data/tools/discovery.py` | Modify | Remove "coming soon" skip logic |
| `src/ontario_data/tools/retrieval.py` | Modify | Add `_download_arcgis_resource_data()`, branch on portal type |
| `src/ontario_data/tools/querying.py` | Modify | Return "not available" for ArcGIS portals on `sql_query`/`query_resource` |
| `src/ontario_data/tools/metadata.py` | Modify | Handle `get_resource_schema` for ArcGIS |
| `tests/test_arcgis_client.py` | **CREATE** | Unit tests for ArcGIS client |
| `tests/test_portal_routing.py` | Modify | Ottawa routing + fan-out tests |

# Ontario Data MCP Server — Design Document

## Overview

A FastMCP server that provides comprehensive access to Ontario's Open Data Catalogue (data.ontario.ca). Built for personal use — optimized for power and depth over discoverability. Uses DuckDB as both a local cache and analytical engine, with full geospatial support.

## Platform

data.ontario.ca is a CKAN 2.8 portal with ~2,939 datasets across 30+ Ontario government ministries. Two main APIs:

- **Catalogue API** — dataset metadata search/browse (package_search, package_show, tag_list, organization_list, etc.)
- **Datastore API** — row-level SQL queries against machine-readable resources (datastore_search, datastore_search_sql)

No authentication required for reads. No documented rate limits.

## Architecture: Monolithic Tool Server

Single FastMCP server with shared DuckDB state. Tools organized by category, composable by the LLM.

```
FastMCP Server
├── CKAN Client (async httpx)
├── DuckDB Cache/Analytics Engine (with spatial extension)
├── Tools (34 across 7 categories)
├── Prompts (3 guided workflows)
└── Resources (3 context providers)
```

## Core Infrastructure

### CKAN Client (ckan_client.py)

Async HTTP client wrapping CKAN 2.8 Action API:
- All Action API endpoints (package_search, package_show, datastore_search, datastore_search_sql, tag_list, organization_list, group_list, resource_show)
- Automatic pagination for large result sets
- Respectful rate limiting (configurable delay)
- Error handling with meaningful messages

### DuckDB Cache & Analytics Engine (cache.py)

Persistent DuckDB at `~/.cache/ontario-data/ontario_data.duckdb`. Dual purpose:

1. **Cache** — downloaded datasets as tables with metadata tracking
2. **Analytics** — SQL queries, aggregations, statistics, spatial queries

Schema:
```sql
_cache_metadata (resource_id, dataset_id, table_name, downloaded_at, expires_at, row_count, size_bytes, source_url)
_dataset_metadata (dataset_id, name, title, org, last_modified, json_blob)
```

Each resource becomes table `ds_{dataset_name_slug}_{resource_id_prefix}`.

### Server Lifespan (server.py)

FastMCP lifespan context manager:
- Opens/creates DuckDB database
- Installs extensions (spatial, httpfs, json)
- Provides shared state to all tools via context

## Tools (34 total)

### 1. Discovery & Search (6 tools)

| Tool | Purpose | Key Parameters |
|------|---------|----------------|
| search_datasets | Full-text search across all datasets | query, filters (org, format, frequency), sort_by, limit |
| list_organizations | List all Ontario ministries/orgs with dataset counts | include_counts |
| list_topics | List all tags/topics with frequency | query (optional filter) |
| get_popular_datasets | Most-viewed/recently-updated datasets | sort (recent/popular), limit |
| search_by_location | Find datasets covering a geographic area | region |
| find_related_datasets | Given a dataset, find related ones by tags/org/topic | dataset_id |

### 2. Metadata & Inspection (5 tools)

| Tool | Purpose | Key Parameters |
|------|---------|----------------|
| get_dataset_info | Full metadata for a dataset | dataset_id_or_name |
| list_resources | List all files/resources in a dataset | dataset_id |
| get_resource_schema | Column names, types, sample values | resource_id |
| get_update_history | Created, last modified, update frequency | dataset_id |
| compare_datasets | Side-by-side metadata comparison | dataset_ids |

### 3. Data Retrieval & Caching (5 tools)

| Tool | Purpose | Key Parameters |
|------|---------|----------------|
| download_resource | Download a resource into DuckDB cache | resource_id, force_refresh |
| list_cached_datasets | Show what's in local DuckDB cache | — |
| refresh_cache | Re-download stale cached datasets | resource_id or all |
| cache_stats | Cache size, table counts, staleness report | — |
| remove_from_cache | Drop cached tables to free space | resource_id or all |

### 4. Data Querying (5 tools)

| Tool | Purpose | Key Parameters |
|------|---------|----------------|
| query_resource | Query via CKAN Datastore API (remote) | resource_id, filters, fields, sort, limit |
| sql_query | Run SQL against CKAN Datastore (remote) | sql |
| query_cached | Run SQL against local DuckDB cache | sql |
| preview_data | Quick look at first N rows | resource_id, rows |
| filter_and_aggregate | Natural-language-friendly filter + agg | resource_id, filters, group_by, aggregate |

### 5. Data Quality (4 tools)

| Tool | Purpose | Key Parameters |
|------|---------|----------------|
| check_data_quality | Null counts, type consistency, outliers per column | resource_id |
| check_freshness | Compare update_frequency vs last_modified | dataset_id |
| validate_schema | Check for schema drift between cached and live | resource_id |
| profile_dataset | Full statistical profile: distributions, cardinality, correlations | resource_id |

### 6. Analytics & Statistics (5 tools)

| Tool | Purpose | Key Parameters |
|------|---------|----------------|
| summarize | Descriptive statistics for numeric columns | resource_id, columns |
| time_series_analysis | Trends, seasonality, change detection | resource_id, date_col, value_col |
| cross_tabulate | Cross-tabulation / pivot tables | resource_id, row_field, col_field, value_field, agg |
| correlation_matrix | Pairwise correlations between numeric columns | resource_id, columns |
| compare_periods | Compare metrics across time periods | resource_id, date_col, period1, period2, metrics |

### 7. Geospatial (4 tools)

| Tool | Purpose | Key Parameters |
|------|---------|----------------|
| load_geodata | Download and cache geospatial resources | resource_id |
| spatial_query | Point-in-polygon, buffer, intersection | resource_id, geometry, operation |
| list_geo_datasets | Find datasets with geospatial resources | format_filter |
| geocode_lookup | Find datasets covering a lat/lng or bbox | lat, lng or bbox |

## Prompts

| Prompt | Purpose |
|--------|---------|
| explore_topic | "I'm interested in {topic}" — search, summarize, suggest deep dives |
| data_investigation | Walk through: schema, quality, stats, insights for a dataset |
| compare_datasets | Side-by-side analysis workflow for 2+ datasets |

## Resources

| Resource | URI | Purpose |
|----------|-----|---------|
| Cached index | ontario://cache/index | List of locally cached datasets with freshness |
| Dataset metadata | ontario://dataset/{id} | Full metadata for a specific dataset |
| Portal stats | ontario://portal/stats | Total datasets, orgs, format breakdown |

## Project Structure

```
data.ontario.ca-mcp/
├── pyproject.toml
├── src/
│   └── ontario_data/
│       ├── __init__.py
│       ├── server.py
│       ├── ckan_client.py
│       ├── cache.py
│       ├── tools/
│       │   ├── __init__.py
│       │   ├── discovery.py
│       │   ├── metadata.py
│       │   ├── retrieval.py
│       │   ├── querying.py
│       │   ├── quality.py
│       │   ├── analytics.py
│       │   └── geospatial.py
│       ├── prompts.py
│       └── resources.py
└── tests/
```

## Dependencies

- fastmcp — MCP server framework
- httpx — async HTTP client for CKAN API
- duckdb — analytical database + cache
- pandas — data manipulation for format conversion before DuckDB ingest
- openpyxl — XLSX reading
- geopandas + shapely — geospatial data handling

## Running

```bash
uv run fastmcp run src/ontario_data/server.py
```

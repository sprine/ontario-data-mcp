<!-- mcp-name: ontario-data-mcp -->

# ontario-data-mcp

> [!IMPORTANT]  
> **Beta:** This project is under active development. The data structure and tool interfaces may change, as may the data sources until v0.1.
> LLM-generated analysis may contain errors. Always verify critical findings against the returned source data.

This is an [MCP server](https://gist.github.com/sprine/3a6f2c30c73cc0fe8a7a472a4af771d3) for discovering, downloading, querying, and analyzing datasets from Ontario's Open Data portals. It allows asking questions of the data in English (or Spanish, Chinese, French, etc).

It currently supports the Ontario, Toronto, and Ottawa portals, and utilizes a shared [DuckDB](https://duckdb.org/) cache for fast SQL queries, statistical analysis, and geospatial operations.

## Contributing

Contributions welcome! To get started, see **Installation** below.

Found a bug? Have an idea? Discovered something interesting?
Open an issue here: https://github.com/sprine/ontario-data-mcp/issues

## Features
* `find` - search across supported Ontario open data portals
* `download` - retrieve and cache datasets
* `query` - run SQL, statistical, and geospatial analysis via DuckDB
* **WIP** A `validate` step to verify query outputs against original source files and metadata.
* A shared DuckDB cache for high-performance analytics

```
Portal APIs find → Dataset download → DuckDB cache → MCP tools (find, download, query)
```

## Installation

### With Claude Code

```bash
claude mcp add ontario-data -- uvx ontario-data-mcp
```

To auto-approve all tool calls (no confirmation prompts), add to your Claude Code settings:

```json
{
  "permissions": {
    "allow": ["mcp:ontario-data:*"]
  }
}
```

All read-only tools are annotated as such. The only destructive tool is `cache_manage`, which removes local cached data (no remote mutations).

<details>
  <summary>With VS Code</summary>

Add to `.vscode/mcp.json`:

```json
{
  "mcpServers": {
    "ontario-data": {
      "command": "uvx",
      "args": ["ontario-data-mcp"]
    }
  }
}
```
</details>

<details>
  <summary>From Source</summary>

```bash
git clone https://github.com/sprine/ontario-data-mcp
cd ontario-data-mcp
uv sync
uv run ontario-data-mcp
```
</details>

## Supported Portals

| Portal | Platform | Datasets |
|--------|----------|----------|
| `ontario` (default) | CKAN | ~5,700 |
| `toronto` | CKAN | ~533 |
| `ottawa` | ArcGIS Hub | ~665 |

## List of tools available to the AI agent

<details>
<summary><b>Portal Management</b> (3 tools)</summary>

| Tool | Description |
|------|-------------|
| `set_portal` | Set the active data portal for subsequent queries |
| `list_portals` | List all available portals with platform type and active marker |
| `search_all_portals` | Search across all portals simultaneously |

</details>

<details>
<summary><b>Discovery</b> (6 tools)</summary>

| Tool | Description |
|------|-------------|
| `search_datasets` | Search for datasets by keyword |
| `list_organizations` | List government ministries with dataset counts |
| `list_topics` | List all tags/topics in the catalogue |
| `get_popular_datasets` | Get popular or recently updated datasets |
| `search_by_location` | Find datasets covering a specific geographic area |
| `find_related_datasets` | Find datasets related by tags and organization |

</details>

<details>
<summary><b>Metadata</b> (4 tools)</summary>

| Tool | Description |
|------|-------------|
| `get_dataset_info` | Get full metadata for a dataset including all resources |
| `list_resources` | List all files in a dataset with formats and sizes |
| `get_resource_schema` | Get column schema and sample values for a datastore resource |
| `compare_datasets` | Compare metadata side-by-side for multiple datasets |

</details>

<details>
<summary><b>Retrieval & Caching</b> (4 tools)</summary>

| Tool | Description |
|------|-------------|
| `download_resource` | Download a resource and cache it locally in DuckDB (returns staleness info if already cached) |
| `cache_info` | Cache statistics + list all cached datasets with staleness |
| `cache_manage` | Remove single resource, clear all, or refresh (action enum) |
| `refresh_cache` | Re-download cached resources with latest data |

</details>

<details>
<summary><b>Querying</b> (4 tools)</summary>

| Tool | Description |
|------|-------------|
| `query_resource` | Query a resource via CKAN Datastore API (remote) |
| `sql_query` | Run SQL against the CKAN Datastore (remote) |
| `query_cached` | Run SQL against locally cached data in DuckDB |
| `preview_data` | Quick preview of first N rows of a resource |

</details>

<details>
<summary><b>Data Quality</b> (3 tools)</summary>

| Tool | Description |
|------|-------------|
| `check_data_quality` | Analyze nulls, type consistency, duplicates, outliers |
| `check_freshness` | Check if a dataset is current vs. its update schedule |
| `profile_data` | Statistical profile using DuckDB SUMMARIZE |

</details>

<details>
<summary><b>Geospatial</b> (3 tools)</summary>

| Tool | Description |
|------|-------------|
| `load_geodata` | Cache a geospatial resource (SHP, KML, GeoJSON) into DuckDB |
| `spatial_query` | Run spatial queries against cached geospatial data |
| `list_geo_datasets` | Find datasets containing geospatial resources |

</details>

## Prompts

Context-aware guided workflow prompts:

- **`explore_topic`** — Guided exploration of a topic (fetches live catalogue context)
- **`data_investigation`** — Deep dive into a specific dataset: schema, quality, statistics
- **`compare_data`** — Side-by-side analysis of multiple datasets

## Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `ONTARIO_DATA_CACHE_DIR` | `~/.cache/ontario-data` | DuckDB storage + log file location |
| `ONTARIO_DATA_TIMEOUT` | `30` | HTTP timeout in seconds |
| `ONTARIO_DATA_RATE_LIMIT` | `10` | Max CKAN requests per second |

## Development

```bash
uv sync
uv run python -m pytest tests/ -v
```

## License

MIT — see [LICENSE](LICENSE) for the software.

Data accessed through this tool is provided under the following open government licences:

- Contains information licensed under the [Open Government Licence – Ontario](https://www.ontario.ca/page/open-government-licence-ontario).
- Contains information licensed under the [Open Government Licence – Toronto](https://open.toronto.ca/open-data-licence/).
- Contains information licensed under the [Open Government Licence – City of Ottawa](https://open.ottawa.ca/pages/open-data-licence).

<!-- mcp-name: ontario-data-mcp -->

# ontario-data-mcp

MCP server for searching, downloading, and analyzing datasets from Ontario's [Open Data Catalogue](https://data.ontario.ca). Caches data locally in DuckDB for fast SQL queries, statistical analysis, and geospatial operations.

## Installation

### With Claude Desktop

Add to your `claude_desktop_config.json`:

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

Config file location:
- macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
- Windows: `%APPDATA%\Claude\claude_desktop_config.json`

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

All 23 read-only tools are annotated as such. The only destructive tool is `cache_manage`, which removes local cached data (no remote mutations).

### With VS Code

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

### From Source

```bash
git clone https://github.com/sprine/ontario-data-mcp
cd ontario-data-mcp
uv sync
uv run ontario-data-mcp
```

## Tools

### Discovery (6 tools)

| Tool | Description |
|------|-------------|
| `search_datasets` | Search for datasets by keyword |
| `list_organizations` | List Ontario government ministries with dataset counts |
| `list_topics` | List all tags/topics in the catalogue |
| `get_popular_datasets` | Get popular or recently updated datasets |
| `search_by_location` | Find datasets covering a specific geographic area |
| `find_related_datasets` | Find datasets related by tags and organization |

### Metadata (4 tools)

| Tool | Description |
|------|-------------|
| `get_dataset_info` | Get full metadata for a dataset including all resources |
| `list_resources` | List all files in a dataset with formats and sizes |
| `get_resource_schema` | Get column schema and sample values for a datastore resource |
| `compare_datasets` | Compare metadata side-by-side for multiple datasets |

### Retrieval & Caching (4 tools)

| Tool | Description |
|------|-------------|
| `download_resource` | Download a resource and cache it locally in DuckDB (returns staleness info if already cached) |
| `cache_info` | Cache statistics + list all cached datasets with staleness |
| `cache_manage` | Remove single resource, clear all, or refresh (action enum) |
| `refresh_cache` | Re-download cached resources with latest data |

### Querying (4 tools)

| Tool | Description |
|------|-------------|
| `query_resource` | Query a resource via CKAN Datastore API (remote) |
| `sql_query` | Run SQL against the CKAN Datastore (remote) |
| `query_cached` | Run SQL against locally cached data in DuckDB |
| `preview_data` | Quick preview of first N rows of a resource |

### Data Quality (3 tools)

| Tool | Description |
|------|-------------|
| `check_data_quality` | Analyze nulls, type consistency, duplicates, outliers |
| `check_freshness` | Check if a dataset is current vs. its update schedule |
| `profile_data` | Statistical profile using DuckDB SUMMARIZE |

### Geospatial (3 tools)

| Tool | Description |
|------|-------------|
| `load_geodata` | Cache a geospatial resource (SHP, KML, GeoJSON) into DuckDB |
| `spatial_query` | Run spatial queries against cached geospatial data |
| `list_geo_datasets` | Find datasets containing geospatial resources |

## Prompts

Context-aware guided workflow prompts:

- **`explore_topic`** — Guided exploration of a topic (fetches live catalogue context)
- **`data_investigation`** — Deep dive into a specific dataset: schema, quality, statistics
- **`compare_data`** — Side-by-side analysis of multiple datasets

## Resources

- `ontario://cache/index` — List all locally cached datasets with freshness info
- `ontario://dataset/{dataset_id}` — Full metadata for a specific dataset
- `ontario://portal/stats` — Overview statistics about the Ontario Data Catalogue
- `ontario://guides/duckdb-sql` — DuckDB SQL reference with tips for Ontario data analysis

## Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `LOG_LEVEL` | `WARNING` | Python logging level |
| `ONTARIO_DATA_CACHE_DIR` | `~/.cache/ontario-data` | DuckDB storage + log file location |
| `ONTARIO_DATA_TIMEOUT` | `30` | HTTP timeout in seconds |
| `ONTARIO_DATA_RATE_LIMIT` | `10` | Max CKAN requests per second |

## How It Works

1. **Search** the Ontario Data Catalogue using CKAN API tools
2. **Download** resources into a local DuckDB database for fast access
3. **Query** cached data with full DuckDB SQL (time series, correlations, pivots, window functions)
4. **Analyze** with statistical profiling, data quality checks, and geospatial queries

Data is cached at `~/.cache/ontario-data/ontario_data.duckdb`. No API keys required — the Ontario Data Catalogue is fully public.

## Development

```bash
uv sync
uv run python -m pytest tests/ -v
```

## License

MIT

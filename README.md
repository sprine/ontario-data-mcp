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
git clone https://github.com/YOUR_USERNAME/ontario-data-mcp
cd ontario-data-mcp
uv sync
uv run ontario-data-mcp
```

## Tools

### Discovery

| Tool | Description |
|------|-------------|
| `search_datasets` | Search for datasets by keyword |
| `list_organizations` | List Ontario government ministries with dataset counts |
| `list_topics` | List all tags/topics in the catalogue |
| `get_popular_datasets` | Get popular or recently updated datasets |
| `search_by_location` | Find datasets covering a specific geographic area |
| `find_related_datasets` | Find datasets related to a given dataset by tags and organization |

### Metadata

| Tool | Description |
|------|-------------|
| `get_dataset_info` | Get full metadata for a dataset including all resources |
| `list_resources` | List all files in a dataset with formats and sizes |
| `get_resource_schema` | Get column schema and sample values for a datastore resource |
| `get_update_history` | Check creation date, last modified, and update frequency |
| `compare_datasets` | Compare metadata side-by-side for multiple datasets |

### Retrieval & Caching

| Tool | Description |
|------|-------------|
| `download_resource` | Download a resource and cache it locally in DuckDB |
| `list_cached_datasets` | List all datasets in the local DuckDB cache |
| `refresh_cache` | Re-download cached resources with latest data |
| `cache_stats` | Get cache statistics: size, table count, staleness |
| `remove_from_cache` | Remove cached data to free disk space |

### Querying

| Tool | Description |
|------|-------------|
| `query_resource` | Query a resource via CKAN Datastore API (remote) |
| `sql_query` | Run SQL against the CKAN Datastore (remote) |
| `query_cached` | Run SQL against locally cached data in DuckDB |
| `preview_data` | Quick preview of first N rows of a resource |
| `filter_and_aggregate` | Filter and aggregate cached data using natural parameters |

### Data Quality

| Tool | Description |
|------|-------------|
| `check_data_quality` | Analyze nulls, type consistency, duplicates, outliers |
| `check_freshness` | Check if a dataset is current vs. its update schedule |
| `validate_schema` | Compare cached schema with the current live version |
| `profile_dataset` | Generate a comprehensive statistical profile |

### Analytics

| Tool | Description |
|------|-------------|
| `summarize` | Descriptive statistics for numeric columns |
| `time_series_analysis` | Analyze trends and patterns in time-indexed data |
| `cross_tabulate` | Create cross-tabulation (pivot table) from cached data |
| `correlation_matrix` | Pairwise correlations between numeric columns |
| `compare_periods` | Compare metrics between two time periods |

### Geospatial

| Tool | Description |
|------|-------------|
| `load_geodata` | Cache a geospatial resource (SHP, KML, GeoJSON) into DuckDB |
| `spatial_query` | Run spatial queries against cached geospatial data |
| `list_geo_datasets` | Find datasets containing geospatial resources |
| `geocode_lookup` | Find datasets covering a geographic point or bounding box |

## Prompts

The server includes guided workflow prompts:

- **`explore_topic`** — Guided exploration of a topic across Ontario's open data
- **`data_investigation`** — Deep dive into a specific dataset: schema, quality, statistics
- **`compare_data`** — Side-by-side analysis of multiple datasets

## Resources

- `ontario://cache/index` — List all locally cached datasets with freshness info
- `ontario://dataset/{dataset_id}` — Full metadata for a specific dataset
- `ontario://portal/stats` — Overview statistics about the Ontario Data Catalogue

## How It Works

1. **Search** the Ontario Data Catalogue using CKAN API tools
2. **Download** resources into a local DuckDB database for fast access
3. **Query** cached data with SQL or use built-in analytics tools
4. **Analyze** with statistical profiling, time series analysis, and geospatial queries

Data is cached at `~/.cache/ontario-data/ontario_data.duckdb`. No API keys required — the Ontario Data Catalogue is fully public.

## Development

```bash
uv sync
uv run pytest tests/ -v
```

## License

MIT

# Ontario Data MCP Server

MCP server for Ontario's Open Data Catalogue (data.ontario.ca).

## Quick Start

```bash
uv sync
uv run pytest tests/ -v
uv run fastmcp run src/ontario_data/server.py
```

## Architecture

- `src/ontario_data/server.py` — FastMCP server with lifespan (DuckDB + CKAN client)
- `src/ontario_data/ckan_client.py` — Async CKAN 2.8 API client
- `src/ontario_data/cache.py` — DuckDB cache manager
- `src/ontario_data/tools/` — 34 tools across 7 categories
- `src/ontario_data/prompts.py` — Guided workflow prompts
- `src/ontario_data/resources.py` — MCP resources

## Testing

```bash
uv run pytest tests/ -v
```

## Key Decisions

- DuckDB for local cache AND analytics (with spatial extension)
- All CKAN API calls go through async httpx client
- Tools return JSON strings for structured LLM consumption
- Geospatial data stored as WKT in DuckDB for spatial queries

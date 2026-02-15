# Ontario Data MCP Server

MCP server for Ontario's Open Data Catalogue (data.ontario.ca).

## Quick Start

```bash
uv sync
uv run python -m pytest tests/ -v
uv run fastmcp run src/ontario_data/server.py
```

## Architecture

- `src/ontario_data/server.py` — FastMCP server with lifespan (DuckDB + CKAN client + logging)
- `src/ontario_data/ckan_client.py` — Async CKAN 2.8 API client with retry and rate limiting
- `src/ontario_data/cache.py` — DuckDB cache manager with SQL validation
- `src/ontario_data/utils.py` — Shared helpers, custom exceptions, context extractors
- `src/ontario_data/staleness.py` — Cache staleness detection (detection-only, no auto-refresh)
- `src/ontario_data/logging_config.py` — Structured JSON logging to rotating file
- `src/ontario_data/tools/` — 24 tools across 6 categories (analytics.py deleted)
- `src/ontario_data/prompts.py` — Context-aware guided workflow prompts
- `src/ontario_data/resources.py` — MCP resources (4 total, including DuckDB SQL guide)

## Testing

```bash
uv run python -m pytest tests/ -v
```

## Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `LOG_LEVEL` | `WARNING` | Python logging level |
| `ONTARIO_DATA_CACHE_DIR` | `~/.cache/ontario-data` | DuckDB storage + log file location |
| `ONTARIO_DATA_TIMEOUT` | `30` | HTTP timeout in seconds |
| `ONTARIO_DATA_RATE_LIMIT` | `10` | Max CKAN requests per second (per session) |

## Key Decisions

- DuckDB for local cache AND analytics (with spatial extension)
- All CKAN API calls go through async httpx client with retry (3 attempts, exponential backoff)
- Tools return JSON strings for structured LLM consumption
- SQL validation: prefix check + semicolon rejection on all user queries
- Geospatial data stored as WKT in DuckDB for spatial queries
- geopandas lazy-imported for faster startup
- Error handling via exceptions (FastMCP preserves messages for LLM)
- Cache staleness: detection only, LLM decides whether to refresh

# Changelog

All notable changes to this project will be documented in this file.

## [0.1.7] - 2026-02-20

### Added

- **Multi-portal support**: federated search across Ontario, Toronto, and Ottawa portals by default. All discovery and data tools fan out across portals unless narrowed with `portal=`.
- **Ottawa ArcGIS Hub**: full portal support via new `ArcGISHubClient` with search, metadata, and download capabilities.
- **`__main__.py`**: run the server with `python -m ontario_data`.
- Per-resource `size_bytes` and `size_mb` in `cache_info` output.
- Licence attribution for all three portals in README and site.

### Changed

- Upgrade to `fastmcp>=3.0.0` (breaking change from 2.x — tools accessed via `get_tool()` API).
- Migrate from private `_lifespan_result` to `ctx.lifespan_context` (FastMCP 3.0 pattern).
- Remove `active_portal` session state — portals are now selected per-call or fan out to all.
- Extract portal resolution helpers to `utils.py`, eliminating ~10x duplicated resolve blocks across tools.
- Centralize geo table naming in `utils.make_geo_table_name`.
- Consolidate cache refresh into single `refresh_cache` method.
- Rewrite tool docstrings to explain "why" rather than restating signatures.
- Rewrite README for multi-portal support.
- Enrich `pyproject.toml`: add `license`, `authors`, `keywords`, `project.urls`, full classifiers.
- Updated project description to reflect multi-portal support.

### Fixed

- Fix `pyproject.toml` structure: move `dependencies` out of `[project.urls]` where it was misplaced, breaking builds.
- Fix zero tools visible to MCP hosts via `fastmcp run`.
- Fix GeoJSON download storing raw geometry and pagination infinite loop.
- Fix prompts and resources that were hardcoded to Ontario portal.
- Fix stale tool references, counts, and patterns across docs, skills, and site.
- Remove stray `coverage` from runtime dependencies.

### Removed

- Remove redundant `search_by_location` and `get_popular_datasets` tools.
- Remove duplicate `FREQUENCY_DAYS` refs and unused `is_stale`.

## [0.1.6] - 2026-02-18

### Added

- Cache CLI for inspecting and managing local DuckDB data (`ontario-data-cache`).
- MCP tool annotations for auto-approve support.
- Claude skills and `/release` command.

### Changed

- Use short-lived DuckDB connections to avoid lock contention.
- Improved tool prompts and fixed semicolon handling to prevent common LLM pitfalls.
- Set `requires-python = ">=3.10"`.
- Site redesign: semantic HTML, keyboard-accessible tool cards, carousel extracted to JSON with JS renderer.

## [0.1.5] - 2026-02-17

### Fixed

- Pin `fastmcp>=2.14.5` to match lifespan state API used since 0.1.4.

### Changed

- Updated site copy and ticker animation.

## [0.1.4] - 2026-02-17

### Fixed

- FastMCP 2.14.5 compatibility: replaced removed `ctx.lifespan_context` with `ctx.fastmcp._lifespan_result`.

## [0.1.3] - 2026-02-17

### Changed

- Single-source version from `pyproject.toml`.
- GitHub Pages deploy workflow.
- Site renamed.

### Fixed

- Pages workflow: add `contents:read` for private repo checkout.

## [0.1.2] - 2026-02-16

### Changed

- Improved documentation with commands in `how_to.md`.
- Isolated tests so they don't conflict with a running server instance.

## [0.1.1] - 2026-02-16

### Changed

- Consolidated tools from 34 to 24 with standardized error handling.
- Added SQL validation with prefix check and semicolon rejection.
- Added utils, file logging, CKAN retry logic, and staleness detection.
- Context-aware prompts, SQL guide resource, and unit tests.

### Added

- MIT license.

## [0.1.0] - 2026-02-15

### Added

- Initial release.
- Async CKAN API client with pagination and error handling.
- DuckDB cache manager with metadata tracking.
- 24 tools: discovery, metadata, retrieval, querying, quality, and geospatial.
- MCP prompts and resources for guided workflows.
- Marketing site with examples, tool cards, and tabbed install section.

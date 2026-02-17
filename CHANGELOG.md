# Changelog

All notable changes to this project will be documented in this file.

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

# Known Issues

Full codebase audit — 2026-03-10. Priorities validated by three independent reviewers.

---

## P1 — Important (real bugs, easy fixes)

### DuckDB file-reading functions accessible via user SQL

`_validate_sql()` allows `SELECT read_csv('/etc/passwd')` — DuckDB file I/O functions pass prefix validation. While this is a local tool (the user already has filesystem access), an LLM under prompt injection could exfiltrate files through this vector.

Fix: `conn.execute("SET enable_external_access = false")` in `_connect()`. One line, comprehensive, no blocklist maintenance. This disables `read_csv`, `read_parquet`, `httpfs`, etc. in one shot.

File: `src/ontario_data/cache.py` (`_connect` method)

---

### No DuckDB query execution timeout

User SQL has no time limit. Accidental cross joins run indefinitely.

Fix: `conn.execute("SET statement_timeout='30s'")` in `_connect()`. One line.

File: `src/ontario_data/cache.py` (`_connect` method)

---

### `infer_portal_from_table` silently falls back to "ontario"

If a cached table name doesn't match `ds_<portal>_` convention, `refresh_cache` uses the Ontario portal for a Toronto/Ottawa resource — produces a confusing 404 from the wrong API.

Fix: raise an error instead of silently falling back.

File: `src/ontario_data/utils.py:237`, `src/ontario_data/tools/retrieval.py:286`

---

### `datastore_search_all()` unbounded memory accumulation

Paginate-and-accumulate pattern with no upper bound. While government APIs are slow (pages of 1000), extremely large resources could eventually OOM.

Fix: hardcoded cap (`if len(all_records) > 500_000: break` with a log warning). No need for a configurable parameter.

File: `src/ontario_data/ckan_client.py:194-227`

---

## P2 — Moderate (fix when convenient)

### `store_resource` not transactional

Drop/create/insert sequence has no explicit transaction. If interrupted mid-sequence, cache is inconsistent. Recovery: user just re-downloads.

Fix: wrap in `BEGIN`/`COMMIT` if trivial. Not urgent — benign failure mode for a single-user embedded DB.

File: `src/ontario_data/cache.py:215-248`

---

### `ArcGISHubClient` has no retry logic

Unlike `CKANClient` (exponential backoff + jitter), ArcGIS has no retry. Also hard-codes 30s timeout ignoring `ONTARIO_DATA_TIMEOUT` env var. Failure mode: user retries manually.

File: `src/ontario_data/arcgis_client.py`

---

### SQL highlighter: double-quoted identifiers not handled

No tokenizer branch for `"..."`. Content between double-quotes is tokenized as normal SQL — `SELECT "FROM"` highlights `FROM` as a keyword.

File: `site/sql-highlight.js:99-125`

---

### Carousel: missing null guard on `punchline` and `tool`

`renderCard` always renders `esc(ex.punchline)` — if missing, outputs string `"undefined"`. Same for `esc(s.tool)`.

File: `site/carousel.js:28, 63-64`

---

## P3 — Low / Cosmetic

### README tool descriptions incomplete

| Tool | Issue |
|------|-------|
| `profile_data` | Doesn't mention duplicate checks or VARCHAR warnings |
| `compare_datasets` | Doesn't mention 2–5 dataset limit |
| `search_datasets` | Only mentions `portal=`; function has 7 params |
| `spatial_query` | Doesn't list the 3 operations |
| WIP `validate` (line 24) | Listed as WIP but zero code exists — remove |

Fix when touching those tools. File: `README.md`

---

### Site minor issues

| Issue |
|-------|
| "6,900+ datasets" count will drift over time |
| FastMCP link in footer may be stale |
| Ticker `url` not passed through `escapeHtml()` (safe since URLs come from static `examples.json`) |
| Clipboard `writeText()` has no `.catch()` |

File: `site/index.html`

---

### `compare_datasets` allows empty/single `dataset_ids`

Docstring says 2–5 datasets but no runtime minimum validation. Empty list produces empty comparison without error.

File: `src/ontario_data/tools/metadata.py:165`

---

### `profile_data` TRY_CAST stats for numeric VARCHARs

Type warnings are surfaced, but no numeric stats computed for VARCHAR columns flagged as numeric. Feature request, not a bug.

File: `src/ontario_data/tools/quality.py`

---

### SEP-1303 error verification test

Missing: a test that calls `query_cached` with invalid SQL and asserts `isError: true` in the MCP response.

---

### Tool description length

`download_resource` (~330 chars) and `profile_data` (~220 chars) docstrings under 400-char target.

Files: `src/ontario_data/tools/retrieval.py`, `src/ontario_data/tools/quality.py`

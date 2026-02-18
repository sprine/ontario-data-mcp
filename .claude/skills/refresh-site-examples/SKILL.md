---
name: refresh-site-examples
description: Refresh landing page examples with real Ontario data. Use when asked to update, add, or refresh examples on the site. Searches the Ontario Data Catalogue via MCP tools to find real datasets, queries actual data to verify every claim, and writes the JSON array to site/examples.json (fetched at runtime by carousel.js).
---

# Refresh Site Examples

Generate real, source-backed examples for the `site/index.html` landing page. Examples are stored in `site/examples.json` and fetched at runtime by `site/carousel.js`. SQL is highlighted by `site/sql-highlight.js`.

## NON-NEGOTIABLE: Factual Correctness

**Every number, statistic, and finding on the site MUST be verified by querying real data. Hallucinations = mistrust. This is the #1 rule.**

- NEVER write a number you haven't queried from real data
- NEVER use "plausible" or "realistic" numbers — only VERIFIED numbers
- If data can't be queried (XLSX-only, 0 resources, rate-limited), write the finding WITHOUT specific numbers — describe what the query reveals instead
- If a cross-dataset claim can't be verified (e.g. one dataset has no downloadable resources), rewrite to use only verifiable data
- After writing any claim, ask yourself: "Did I run a query that returned this exact number?" If no, delete the number.

## When to use

- User asks to update, refresh, add, or replace examples on the site
- User wants new examples for a specific topic or section
- User wants to make examples "real" or add sources

## JSON schema

Each example is an object in the array:

```json
{
  "tag": "Time series",
  "question": "Natural language question a person would ask?",
  "answer": "Narrative of what the tool does — dataset name, resource count, analysis technique.",
  "punchline": "The bold finding, verified by real query.",
  "sources": [
    {
      "title": "Exact Dataset Title from get_dataset_info",
      "url": "https://data.ontario.ca/dataset/{slug}",
      "org": "Organization Name from get_dataset_info"
    }
  ],
  "hood": {
    "steps": [
      { "tool": "search_datasets", "description": "dataset_id_first_8_chars" },
      { "tool": "download_resource", "description": "17 CSVs" },
      { "tool": "query_cached", "description": "" }
    ],
    "sql": "SELECT col_name,\n  SUM(TRY_CAST(other_col AS INTEGER)) AS total\nFROM table_name\nGROUP BY col_name\nORDER BY total DESC"
  },
  "followup": {
    "question": "Follow-up question?",
    "answer": "Follow-up narrative.",
    "punchline": "Follow-up finding (if verified).",
    "sources": [{ "title": "...", "url": "...", "org": "..." }]
  }
}
```

### Required fields

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `tag` | string | yes | One of: "Time series", "Cross-dataset", "Geospatial", "Investigation", "Scale callout" |
| `question` | string | yes | Natural-language question |
| `answer` | string | yes | Narrative (no HTML — plain text only) |
| `punchline` | string | yes | Bold finding, verified by query (no HTML) |
| `sources` | array | yes | At least one `{title, url, org}` |
| `sources[].title` | string | yes | Exact title from `get_dataset_info` |
| `sources[].url` | string | yes | Portal dataset URL — e.g. `https://data.ontario.ca/dataset/{slug}` or `https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/{slug}` |
| `sources[].org` | string | yes | Exact organization from `get_dataset_info` |
| `hood` | object | yes | Under-the-hood section |
| `hood.steps` | array | yes | At least one `{tool, description}` |
| `hood.steps[].tool` | string | yes | MCP tool name |
| `hood.steps[].description` | string | no | Annotation (dataset ID, count, etc.) |
| `hood.sql` | string | yes | Plain SQL string — highlighted at runtime by sql-highlight.js |
| `followup` | object | no | Optional nested follow-up |

### How the renderer works

- `carousel.js` reads the JSON from `<script id="examples-data">`, builds carousel cards, and injects into `.carousel-track`
- `sql-highlight.js` auto-highlights SQL keywords (bold), functions, strings (italic), numbers, and comments (italic) using Dracula color scheme
- The section starts hidden (`display:none`) and is shown after rendering
- No manual `<span>` tags needed — write plain SQL in the `hood.sql` field

## Process — follow these steps in order

### Step 1: Pick topics

Choose 6 topics. Aim for variety across tag types:
- **Time series** — trends over time
- **Cross-dataset** — joins across 2+ datasets
- **Geospatial** — spatial queries, boundaries, radius
- **Investigation** — profiling, drilling down into one dataset
- **Scale callout** — e.g. "6,000+ facilities", "439K records"

### Step 2: Search the catalogue via MCP

For each topic, use the `ontario-data` MCP tools. You can search across portals:

1. **`search_all_portals`** — discover which portal has the best data for a topic
2. **`set_portal`** — switch to the portal you want (or pass `portal=` to individual tools)
3. **`search_datasets`** — find candidates by keyword
4. **`get_dataset_info`** — get the dataset ID, title, organization, update frequency
5. **`list_resources`** — get resource IDs and formats
6. **`get_resource_schema`** — get real column names (critical for realistic SQL)

Collect for each dataset:
- **Title** (exact, verbatim from `get_dataset_info`)
- **Dataset URL**: `https://{portal-base-url}/dataset/{slug}` — use the slug, NOT the UUID. For Ontario: `https://data.ontario.ca/dataset/{slug}`. For Toronto: `https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/{slug}`.
- **Dataset ID** (first 8 chars, for hood-step descriptions)
- **Column names** (for realistic SQL)
- **Organization** (verbatim from `get_dataset_info`)

### Step 3: Write the question

Good questions are:
- Natural language a real person would type
- Specific enough to imply a clear analysis
- Interesting enough that someone would want to know the answer

Bad questions: "Show me data about X" (too vague), "Query the Y table" (too technical)

### Step 4: Query the data to get real findings

**Before writing any narrative, you MUST query the actual data.** This is non-negotiable.

For each example:
1. **`download_resource`** — cache the resource(s) locally in DuckDB
2. **`query_cached`** — run the actual analysis query to get real numbers
3. Record the exact results — these become your punchline

**Common data gotchas:**
- Column names may differ across resources in the same dataset (e.g. `TotalEV` vs `Total EV`)
- Values may be stored as text — use `TRY_CAST(col AS DOUBLE)`
- A column like `"No. of Exceedances"` may hold counts per row — `SUM()` not `COUNT(*)`
- Units differ between datasets (mg/L vs µg/L) — always check the unit column
- Semicolons in column values break `query_cached` — use `LIKE` patterns
- `sql_query` (remote) hits 429 rate limits — prefer `download_resource` + `query_cached` (local)

### Step 5: Write the JSON

For each example, create an object matching the schema above. Key rules:
- `answer` and `punchline` are separate fields — no HTML in either
- `hood.sql` is a plain SQL string with `\n` for newlines — no `<span>` tags
- Sources must have exact titles, real slugs, and verbatim organization names
- Steps use `{tool, description}` — description is free text (dataset ID, count, format)

### Step 6: Validate before writing

Before writing the JSON to the page, check every example:

1. **Required fields present:** tag, question, answer, punchline, sources (≥1), hood.steps (≥1), hood.sql
2. **URLs match pattern:** every `sources[].url` is a valid portal dataset URL (e.g. `https://data.ontario.ca/dataset/...` or `https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/...`)
3. **No empty punchlines:** every punchline is a non-empty string
4. **Tag variety:** not all examples use the same tag
5. **SQL is plain text:** no `<span>`, `<strong>`, or other HTML in `hood.sql`
6. **No fabricated numbers:** every number in a punchline was returned by a real query

If any check fails, fix the issue before writing.

### Step 7: Write the JSON file

Use the Write tool to overwrite `site/examples.json` with the full array. Do NOT touch `site/index.html` or any other files — the carousel renderer fetches `examples.json` at runtime.

## Available MCP tools reference

Tool names for `hood.steps[].tool`:

**Portal:** set_portal, list_portals, search_all_portals
**Discovery:** search_datasets, list_organizations, list_topics, get_popular_datasets, search_by_location, find_related_datasets
**Metadata:** get_dataset_info, list_resources, get_resource_schema, compare_datasets
**Retrieval:** download_resource, cache_info, cache_manage, refresh_cache
**Querying:** query_resource, sql_query, query_cached, preview_data
**Quality:** check_data_quality, check_freshness, profile_data
**Geospatial:** load_geodata, spatial_query, list_geo_datasets

## Quality checklist

Before finishing, verify:

### Factual correctness (MOST IMPORTANT)
- [ ] **Every number in a punchline was returned by a real query** — no exceptions
- [ ] Every ratio/comparison uses matching units (check mg/L vs µg/L, counts vs sums)
- [ ] Cross-dataset claims use datasets that actually have downloadable resources
- [ ] If data couldn't be queried, the finding uses NO specific numbers
- [ ] `COUNT(*)` vs `SUM(quantity_column)` — verified which is correct for each dataset

### JSON structure
- [ ] Every source URL uses a real portal dataset slug (not a UUID)
- [ ] Column names in SQL match what `get_resource_schema` returned
- [ ] SQL is valid DuckDB syntax (not MySQL/Postgres-specific)
- [ ] SQL is plain text — no HTML tags
- [ ] Each example has all required fields
- [ ] Tags are varied (not all the same type)
- [ ] JSON is valid (parseable by `JSON.parse`)

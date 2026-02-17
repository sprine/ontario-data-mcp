---
name: refresh-site-examples
description: Refresh landing page examples with real Ontario data. Use when asked to update, add, or refresh examples on the site/index.html landing page. Searches the Ontario Data Catalogue via MCP tools to find real datasets, builds source-backed examples with collapsible SQL details, and writes them into the page.
---

# Refresh Site Examples

Generate real, source-backed examples for the `site/index.html` landing page. Every example must link to a real dataset on `data.ontario.ca` and include the actual SQL/tool chain that would produce the answer.

## When to use

- User asks to update, refresh, add, or replace examples on the site
- User wants new examples for a specific topic or section
- User wants to make examples "real" or add sources

## Page structure

The landing page has two example sections with different formats:

### 1. "You ask. It translates." — full-width stacked cards

```html
<div class="example">
  <div class="example-tag">TAG</div>
  <div class="example-q">Natural language question?</div>
  <div class="example-a">Narrative of what happens. <strong>The bold punchline finding.</strong></div>
  <div class="example-sources">Source: <a href="URL" target="_blank" rel="noopener">Dataset Title</a> — Ministry Name</div>
  <details>
    <summary>Under the hood</summary>
    <div class="hood-content">
      <div class="hood-step"><span class="tool-name">tool_name</span> → description</div>
      <div class="hood-code">SQL with syntax highlighting spans</div>
    </div>
  </details>
</div>
```

### 2. "Go deeper with follow-ups." — horizontal carousel cards

```html
<div class="carousel-card">
  <div class="carousel-tag">TAG</div>
  <div class="carousel-q">Question?</div>
  <div class="carousel-a">Short answer. One sentence finding.</div>
  <div class="carousel-sources"><a href="URL" target="_blank" rel="noopener">Dataset Title</a></div>
  <details>
    <summary>Under the hood</summary>
    <div class="hood-content">
      <div class="hood-step"><span class="tool-name">tool_name</span> → description</div>
      <div class="hood-code">SQL with syntax highlighting spans</div>
    </div>
  </details>
</div>
```

## SQL syntax highlighting classes

Use these `<span>` classes inside `.hood-code` blocks:

| Class | Color | Use for |
|-------|-------|---------|
| `kw` | purple (#c792ea / #ff79c6) | SQL keywords: SELECT, FROM, WHERE, JOIN, GROUP BY, ORDER BY, WITH, AS, ON, LIMIT, HAVING, BETWEEN, AND, OR, DESC, ASC, PARTITION BY, OVER |
| `fn` | blue (#82aaff / #8be9fd) | Functions: AVG, SUM, COUNT, ROUND, LAG, FIRST, LAST, YEAR, LEFT, RANK, ST_Contains, ST_Point |
| `str` | green/yellow (#c3e88d / #f1fa8c) | String literals: 'PHOSPHORUS', 'CTAS 1' |
| `num` | orange/purple (#f78c6c / #bd93f9) | Numbers: 1.0, 10, 20 |
| `comment` | dim (#545454 / #6272a4) | Comments: -- explanation, // note |

Note: The two color schemes differ between sections (main uses Material, carousel uses Dracula).

## Process — follow these steps in order

### Step 1: Pick topics

Choose 6 topics for the main section. Aim for variety across these tag types:
- **Time series** — trends over time
- **Cross-dataset** — joins across 2+ datasets
- **Geospatial** — spatial queries, boundaries, radius
- **Investigation** — profiling, drilling down into one dataset
- **Scale callout** — e.g. "170 datasets", "440K records"

The carousel section uses 6 shorter cards. These can reference follow-up questions that build on the main examples, or demonstrate tool workflows (prompts, freshness, discovery).

### Step 2: Search the catalogue via MCP

For each topic, use the `ontario-data` MCP tools:

1. **`search_datasets`** — find candidates by keyword
2. **`get_dataset_info`** — get the dataset ID, title, organization, update frequency
3. **`list_resources`** — get resource IDs and formats
4. **`get_resource_schema`** — get real column names (critical for realistic SQL)

Collect for each dataset:
- **Title** (exact, for the source link text)
- **Dataset URL**: `https://data.ontario.ca/dataset/{slug}` — use the slug from the dataset info, NOT the UUID
- **Dataset ID** (short form, first 8 chars, for the hood-step `<code>` tags)
- **Resource IDs** (for hood-step references)
- **Column names** (for realistic SQL)
- **Organization/Ministry** (for the source attribution)
- **Resource count and format** (for the narrative)

### Step 3: Write the question

Good questions are:
- Natural language a real person would type
- Specific enough to imply a clear analysis
- Interesting enough that someone would want to know the answer

Bad questions: "Show me data about X" (too vague), "Query the Y table" (too technical)

### Step 4: Write the answer narrative

Structure: `[What the tool does] → [bold punchline finding]`

The narrative should:
- Mention the dataset name, resource count, and format
- Describe the analysis technique (time series, join, spatial query)
- End with a `<strong>` block containing the specific finding
- Use real numbers that are plausible for the dataset

### Step 5: Add sources

Format: `Source: <a href="URL">Exact Dataset Title</a> — Ministry/Organization`

For cross-dataset examples: `Sources: <a href="URL1">Title 1</a>, <a href="URL2">Title 2</a>`

The URL MUST be a real `data.ontario.ca/dataset/` link. Verify by checking the slug returned from `get_dataset_info`.

### Step 6: Build the "Under the hood" section

Show the actual chain of tool calls, then the SQL:

1. List each tool call as a `hood-step` with the tool name in a `tool-name` span
2. Include the short dataset ID in a `<code style="color:#666">` tag
3. Show real column names from Step 2
4. Write the SQL in a `hood-code` div with syntax highlighting spans
5. The SQL should use real column names, realistic WHERE clauses, and DuckDB-compatible syntax

### Step 7: Edit the HTML

Use the Edit tool to replace the example content. Do NOT rewrite CSS or surrounding structure — only touch the content inside the `<div class="examples">` or `<div class="carousel-track">` containers.

## Available MCP tools reference

These are the tool names to use in `hood-step` elements:

**Discovery:** search_datasets, list_organizations, list_topics, get_popular_datasets, search_by_location, find_related_datasets
**Metadata:** get_dataset_info, list_resources, get_resource_schema, compare_datasets
**Retrieval:** download_resource, cache_info, cache_manage, refresh_cache
**Querying:** query_resource, sql_query, query_cached, preview_data
**Quality:** check_data_quality, check_freshness, profile_data
**Geospatial:** load_geodata, spatial_query, list_geo_datasets
**Prompts:** explore_topic, data_investigation, compare_data
**Resources:** ontario://cache/index, ontario://dataset/{id}, ontario://portal/stats, ontario://guides/duckdb-sql

## Quality checklist

Before finishing, verify:

- [ ] Every source URL uses a real `data.ontario.ca/dataset/` slug (not a UUID)
- [ ] Every dataset ID in `<code>` tags matches a real dataset
- [ ] Column names in SQL match what `get_resource_schema` returned
- [ ] SQL is valid DuckDB syntax (not MySQL/Postgres-specific)
- [ ] Each example has all three layers: answer, sources, under-the-hood
- [ ] Tags are varied (not all "Cross-dataset")
- [ ] No broken HTML nesting (check `<details>` closure)
- [ ] Bold punchline findings use `<strong>` tags and contain specific numbers
- [ ] Site meets the accessibility standards of https://www.ontario.ca/page/accessibility-in-ontario.

# Marketing Site Design — ontario-data-mcp

## Summary

Single-page marketing site in `_site/index.html` using Tailwind CSS via CDN. Targets a broad audience: data journalists, civic tech developers, and MCP enthusiasts. Warm, accessible tone with Ontario-inspired colors.

## Decisions

- **Audience:** Broad — researchers, developers, AI/MCP users
- **Tone:** Warm and accessible (Notion/Linear style, not corporate)
- **Tech:** Single HTML file, Tailwind via CDN, inline SVG icons, no build step
- **Structure:** Single long-scroll page (Approach A)
- **Hero focus:** Lead with breadth — "34 tools for Ontario open data"

## Visual Design

- **Background:** Warm white `#FAFAF8`
- **Primary:** Deep teal `#0B6E4F` (Ontario parks/nature)
- **Accent:** Warm gold `#D4A843` (CTAs, highlights)
- **Text:** Near-black `#1A1A1A`
- **Typography:** System font stack, large confident headings, relaxed line-height
- **Style:** Light, airy, generous whitespace. Subtle rounded cards with light borders. No gradients or heavy shadows.
- **Icons:** Inline SVG, one per tool category

## Page Sections

### 1. Hero

- Headline: "34 tools for Ontario open data"
- Subhead: "Search, cache, and analyze thousands of datasets from Ontario's Open Data Catalogue — right from Claude. No API keys. One install."
- Primary CTA: Copy-paste install command (`uvx ontario-data-mcp`) with click-to-copy
- Secondary CTA: "View on GitHub" link
- No image — typography and whitespace

### 2. Tool Categories

- 7 cards in responsive grid (3 col desktop, 1 col mobile)
- Each card: SVG icon + category name + tool count + one-sentence description
- Categories: Discovery (6), Metadata (5), Retrieval (5), Querying (5), Data Quality (4), Analytics (5), Geospatial (4)
- Clicking a card expands to show individual tool names (JS toggle)

### 3. How It Works

- 3-step horizontal flow (vertical on mobile):
  1. Search — "Find datasets by keyword, topic, organization, or location"
  2. Cache — "Download into a local DuckDB database for fast access"
  3. Analyze — "Run SQL queries, statistical profiles, and spatial operations"
- Numbered circles with connecting lines

### 4. Example Prompts

- 3-4 chat-bubble-style cards:
  - "Find datasets about housing prices in Toronto"
  - "Download the Ontario sunshine list and show me the top earners"
  - "Compare water quality datasets from the last 5 years"
  - "Find geospatial data that covers Ottawa and load it for spatial queries"

### 5. Install

- Tabbed interface: Claude Desktop / Claude Code / VS Code
- Each tab: exact config JSON or command, copy-paste ready
- "From Source" note below for developers

### 6. Footer

- Links: GitHub, Ontario Data Catalogue, MIT License
- "Built with FastMCP and DuckDB"

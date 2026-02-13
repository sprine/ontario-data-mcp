# Marketing Site Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a single-page marketing site for ontario-data-mcp in `_site/index.html`.

**Architecture:** One HTML file using Tailwind CSS via CDN. Inline SVG icons. Minimal inline JS for copy-to-clipboard and tab switching. No build step.

**Tech Stack:** HTML, Tailwind CSS (CDN), vanilla JS

---

### Task 1: Scaffold and Hero Section

**Files:**
- Create: `_site/index.html`

**Step 1: Create the HTML scaffold with hero**

Create `_site/index.html` with:
- DOCTYPE, head with Tailwind CDN (`<script src="https://cdn.tailwindcss.com"></script>`)
- Tailwind config block extending colors: `primary: '#0B6E4F'`, `accent: '#D4A843'`, `surface: '#FAFAF8'`, `ink: '#1A1A1A'`
- System font stack via Tailwind config
- `<body class="bg-surface text-ink">`
- Hero section:
  - Centered content, generous vertical padding (`py-24`)
  - `<h1>` "34 tools for Ontario open data" — large, bold
  - `<p>` subhead: "Search, cache, and analyze thousands of datasets from Ontario's Open Data Catalogue — right from Claude. No API keys. One install."
  - Install command bar: a rounded container with `uvx ontario-data-mcp` in monospace and a "Copy" button (JS click-to-copy)
  - "View on GitHub" secondary link below

**Step 2: Open in browser and verify**

Run: `open _site/index.html`
Expected: Warm white page, teal heading, install bar with copy button, GitHub link.

**Step 3: Commit**

```bash
git add _site/index.html
git commit -m "feat(site): scaffold and hero section"
```

---

### Task 2: Tool Categories Section

**Files:**
- Modify: `_site/index.html`

**Step 1: Add tool categories grid below hero**

Add a section with:
- Section heading: "What's Inside" centered
- 7 cards in a responsive grid (`grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6`)
- Each card is a `<div>` with: light border, rounded corners, padding, cursor-pointer
  - Inline SVG icon (24x24, stroke style, teal color):
    - Discovery: magnifying glass
    - Metadata: info circle
    - Retrieval: download/database
    - Querying: terminal/code
    - Data Quality: check-shield
    - Analytics: bar chart
    - Geospatial: map pin
  - Category name in bold
  - Tool count in accent color (e.g., "6 tools")
  - One-sentence description
  - Hidden `<div>` with individual tool names as a comma-separated list
- JS: clicking a card toggles visibility of the tool names div, rotates a small chevron

**Step 2: Open in browser and verify**

Expected: Grid of 7 cards. Clicking a card expands to show tool names. Responsive on resize.

**Step 3: Commit**

```bash
git add _site/index.html
git commit -m "feat(site): tool categories section with expandable cards"
```

---

### Task 3: How It Works Section

**Files:**
- Modify: `_site/index.html`

**Step 1: Add 3-step flow section**

Add a section with:
- Section heading: "How It Works" centered
- 3 columns on desktop (`md:grid-cols-3`), stacked on mobile
- Each step:
  - A numbered circle (1, 2, 3) in teal with white text, centered
  - Step name bold below: "Search", "Cache", "Analyze"
  - Description text below:
    1. "Find datasets by keyword, topic, organization, or location"
    2. "Download into a local DuckDB database for fast access"
    3. "Run SQL queries, statistical profiles, and spatial operations"
- Connecting horizontal lines between circles on desktop (use a `border-t` on a connecting div, hidden on mobile)

**Step 2: Open in browser and verify**

Expected: Three steps with numbered circles, connecting lines on desktop, stacked on mobile.

**Step 3: Commit**

```bash
git add _site/index.html
git commit -m "feat(site): how it works section"
```

---

### Task 4: Example Prompts Section

**Files:**
- Modify: `_site/index.html`

**Step 1: Add example prompts section**

Add a section with:
- Section heading: "Try Asking Claude" centered
- 4 prompt cards in a 2x2 grid (`md:grid-cols-2 gap-4`)
- Each card styled as a chat bubble:
  - Light teal background (`bg-primary/5`), rounded-xl, padding
  - A small "user" label or chat icon in muted text
  - The prompt text in quotes, slightly larger font:
    1. "Find datasets about housing prices in Toronto"
    2. "Download the Ontario sunshine list and show me the top earners"
    3. "Compare water quality datasets from the last 5 years"
    4. "Find geospatial data that covers Ottawa and load it for spatial queries"

**Step 2: Open in browser and verify**

Expected: 4 chat-bubble-style cards with example prompts.

**Step 3: Commit**

```bash
git add _site/index.html
git commit -m "feat(site): example prompts section"
```

---

### Task 5: Install Section

**Files:**
- Modify: `_site/index.html`

**Step 1: Add tabbed install section**

Add a section with:
- Section heading: "Get Started" centered
- 3 tab buttons in a row: "Claude Desktop", "Claude Code", "VS Code"
  - Active tab: teal background, white text
  - Inactive: light border, teal text
- Tab content panels (only one visible at a time):
  - **Claude Desktop:** JSON config block in a `<pre>` with the mcpServers config
  - **Claude Code:** `claude mcp add ontario-data -- uvx ontario-data-mcp`
  - **VS Code:** JSON config block for `.vscode/mcp.json`
- Each panel has a "Copy" button (reuse the copy-to-clipboard JS pattern)
- Below tabs: a muted "From Source" note with git clone + uv sync commands

**Step 2: JS for tab switching**

Vanilla JS: clicking a tab shows its panel, hides others, updates active tab styling.

**Step 3: Open in browser and verify**

Expected: 3 tabs, switching shows different install instructions, copy buttons work.

**Step 4: Commit**

```bash
git add _site/index.html
git commit -m "feat(site): tabbed install section"
```

---

### Task 6: Footer

**Files:**
- Modify: `_site/index.html`

**Step 1: Add footer**

Add a `<footer>` with:
- Muted background (slightly darker than surface, e.g., `bg-stone-100`)
- Centered content with three links: GitHub, Ontario Data Catalogue, MIT License
- "Built with FastMCP and DuckDB" in small muted text
- Reasonable padding

**Step 2: Open in browser and verify**

Expected: Clean footer with links and attribution.

**Step 3: Commit**

```bash
git add _site/index.html
git commit -m "feat(site): footer"
```

---

### Task 7: Polish Pass

**Files:**
- Modify: `_site/index.html`

**Step 1: Responsive and visual polish**

- Verify all sections look good at mobile (375px), tablet (768px), desktop (1280px)
- Add smooth scroll behavior (`scroll-behavior: smooth`)
- Add `<meta>` tags: description, viewport, og:title, og:description for social sharing
- Add a favicon (inline SVG data URI of a simple maple leaf or data icon)
- Ensure all interactive elements have hover/focus states

**Step 2: Verify end-to-end**

Open in browser. Scroll through entire page. Test:
- Copy buttons work
- Tab switching works
- Card expansion works
- Responsive at all breakpoints

**Step 3: Commit**

```bash
git add _site/index.html
git commit -m "feat(site): responsive polish and meta tags"
```

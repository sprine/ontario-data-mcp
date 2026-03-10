"""Verify README.md and site/index.html stay in sync with the codebase.

The codebase is always the source of truth. Tests compare docs against code.
These tests are expected to fail when docs drift — fix the docs, not the tests.
"""

from __future__ import annotations

import os
import re
import tomllib
from pathlib import Path

import pytest
from fastmcp import Client

from ontario_data.portals import PORTALS
from ontario_data.server import mcp

ROOT = Path(__file__).resolve().parent.parent
_README = (ROOT / "README.md").read_text(encoding="utf-8")
_HTML = (ROOT / "site" / "index.html").read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _site_stats(html: str) -> dict[str, int]:
    """Extract {label: count} from the stats ribbon data-target attributes."""
    targets = re.findall(r'data-target="(\d+)"', html)
    labels = re.findall(r'class="stat-label">(.*?)<', html)
    return dict(zip(labels, map(int, targets)))


_CARD_RE = re.compile(
    r'class="tool-card-name"[^>]*>(.*?)</span>.*?'
    r'class="tool-card-detail"[^>]*>(.*?)</div>',
    re.DOTALL,
)


def _site_tool_names(html: str) -> set[str]:
    """Extract tool names from tool-category cards only (skip Prompts/Resources)."""
    names: set[str] = set()
    for card_name, detail in _CARD_RE.findall(html):
        if card_name.strip() in ("Prompts", "Resources"):
            continue
        text = re.sub(r"<[^>]+>", "\n", detail)
        for chunk in text.replace("\n", ",").split(","):
            name = chunk.strip()
            if name:
                names.add(name)
    return names


def _readme_tool_names(text: str) -> set[str]:
    """Extract tool names from README tool tables (inside <details> blocks only)."""
    # Scope to the tools section to avoid matching env vars, portal keys, etc.
    tools_section = re.search(
        r"## List of tools.*?(?=\n## )", text, re.DOTALL
    )
    if tools_section is None:
        return set()
    return set(re.findall(r"\|\s*`(\w+)`\s*\|", tools_section.group()))


_CATEGORY_RE = re.compile(r"<summary><b>[\w\s&]+</b>\s*\((\d+)\s+tools?\)", re.I)


def _readme_category_total(text: str) -> int | None:
    matches = _CATEGORY_RE.findall(text)
    return sum(int(n) for n in matches) if matches else None


# ---------------------------------------------------------------------------
# Fixture: introspect MCP server once for all tests
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
async def registry(tmp_path_factory):
    """Introspect the MCP server once for all doc-accuracy tests."""
    cache_dir = tmp_path_factory.mktemp("docs_accuracy_cache")
    old = os.environ.get("ONTARIO_DATA_CACHE_DIR")
    os.environ["ONTARIO_DATA_CACHE_DIR"] = str(cache_dir)
    try:
        async with Client(mcp) as client:
            tools = await client.list_tools()
            resources = await client.list_resources()
            templates = await client.list_resource_templates()
            prompts = await client.list_prompts()
        return {
            "tool_names": {t.name for t in tools},
            "resource_count": len(resources) + len(templates),
            "prompt_count": len(prompts),
        }
    finally:
        if old is None:
            os.environ.pop("ONTARIO_DATA_CACHE_DIR", None)
        else:
            os.environ["ONTARIO_DATA_CACHE_DIR"] = old


# ---------------------------------------------------------------------------
# Tools: README
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_readme_tools_match(registry):
    """README tool name set == registered tool name set."""
    registered = registry["tool_names"]
    documented = _readme_tool_names(_README)
    missing = registered - documented
    extra = documented - registered
    assert not missing and not extra, (
        f"README.md tool table out of sync.\n"
        f"  Missing from README: {sorted(missing)}\n"
        f"  Ghost in README:     {sorted(extra)}\n"
        f"  Fix: update tool tables in README.md"
    )


@pytest.mark.asyncio
async def test_readme_category_counts(registry):
    """README category count headers sum to actual tool count."""
    total = _readme_category_total(_README)
    if total is None:
        pytest.skip("No category counts found in README")
    actual = len(registry["tool_names"])
    assert total == actual, (
        f"README category counts sum to {total} but {actual} tools registered.\n"
        f"  Fix: update the (N tools) counts in README.md"
    )


# ---------------------------------------------------------------------------
# Tools: site
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_site_tools_match(registry):
    """Site tool card names == registered tool name set (tool cards only)."""
    registered = registry["tool_names"]
    documented = _site_tool_names(_HTML)
    missing = registered - documented
    extra = documented - registered
    assert not missing and not extra, (
        f"site/index.html tool cards out of sync.\n"
        f"  Missing from site: {sorted(missing)}\n"
        f"  Ghost in site:     {sorted(extra)}\n"
        f"  Fix: update tool-card-detail divs in site/index.html"
    )


@pytest.mark.asyncio
async def test_site_counts(registry):
    """Stats ribbon counts for tools, resources, prompts all match actual."""
    stats = _site_stats(_HTML)
    actual_tools = len(registry["tool_names"])
    errors = []

    # Tool count in ribbon
    ribbon_tools = stats.get("tools")
    if ribbon_tools is not None and ribbon_tools != actual_tools:
        errors.append(
            f'Tools ribbon says {ribbon_tools}, actual {actual_tools}. '
            f'Fix: data-target="{actual_tools}"'
        )

    # Tool count in heading
    heading = re.search(r'<h2 id="tools-heading">(\d+)\s+tools', _HTML)
    if heading and int(heading.group(1)) != actual_tools:
        errors.append(
            f'Heading says {heading.group(1)}, actual {actual_tools}. '
            f'Fix: update h2#tools-heading'
        )

    # Resource count
    ribbon_res = stats.get("resources")
    actual_res = registry["resource_count"]
    if ribbon_res is not None and ribbon_res != actual_res:
        errors.append(
            f'Resources ribbon says {ribbon_res}, actual {actual_res}. '
            f'Fix: data-target="{actual_res}"'
        )

    # Prompt count
    ribbon_prompts = stats.get("prompts")
    actual_prompts = registry["prompt_count"]
    if ribbon_prompts is not None and ribbon_prompts != actual_prompts:
        errors.append(
            f'Prompts ribbon says {ribbon_prompts}, actual {actual_prompts}. '
            f'Fix: data-target="{actual_prompts}"'
        )

    assert not errors, (
        "site/index.html counts stale:\n"
        + "\n".join(f"  - {e}" for e in errors)
    )


# ---------------------------------------------------------------------------
# Licence attribution
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("portal_key", list(PORTALS.keys()))
async def test_licence_attribution(portal_key):
    """Each portal's licence name + URL appear in README, URL in site."""
    cfg = PORTALS[portal_key]
    assert cfg.licence_name in _README, (
        f"Licence name for '{portal_key}' not in README.md.\n"
        f"  Expected: {cfg.licence_name!r}"
    )
    assert cfg.licence_url in _README, (
        f"Licence URL for '{portal_key}' not in README.md.\n"
        f"  Expected: {cfg.licence_url!r}"
    )
    assert cfg.licence_url in _HTML, (
        f"Licence URL for '{portal_key}' not in site/index.html.\n"
        f"  Expected: {cfg.licence_url!r}"
    )


# ---------------------------------------------------------------------------
# Version consistency
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_no_stale_version_disclaimer():
    """README doesn't say 'until vX.Y' for a version already reached."""
    with open(ROOT / "pyproject.toml", "rb") as f:
        current = tomllib.load(f)["project"]["version"]
    major, minor, *_ = (int(x) for x in current.split("."))
    for m in re.finditer(r"(?:until|before)\s+v(\d+)\.(\d+)", _README):
        threshold = (int(m.group(1)), int(m.group(2)))
        assert (major, minor) < threshold, (
            f"README says '{m.group(0)}' but current version is {current}.\n"
            f"  Fix: remove or update the disclaimer in README.md"
        )

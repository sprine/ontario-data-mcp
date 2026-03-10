# Release Command Review

Review of `.claude/commands/release.md` by three parallel agents (2025-03-09).

See also: [docs-audit.md](docs-audit.md) for README & site accuracy issues that should be checked before every release.

---

## Completed

The following issues were fixed in the release command rewrite (2026-03-09):

- **#1–2** (wrong log string / wrong stream) — removed fragile server startup smoke test; unit tests cover initialization
- **#3–4** (PyPI token leak / brittle .pypirc parsing) — replaced with bare `uv publish` which reads `~/.pypirc` natively
- **#5** (no dirty-working-tree check) — added preflight `git diff --quiet`
- **#6** (no branch check) — added preflight + re-check before push
- **#7** (flaky PyPI verification) — replaced `--dry-run` install with printing PyPI URL
- **#8** (`uv build` too late) — moved build to before commit/tag
- **#9** (redundant smoke test) — removed entirely
- **#11** (licence check as unit test) — now in `tests/test_docs_accuracy.py`, run by `uv run pytest`
- **#12** (three confirmations) — collapsed commit+push into one step, publish as second gate
- **#13** (verification adds little value) — replaced with URL printout
- **#14** (clean dist own step) — folded `rm -rf dist` into build step
- **#15** (no rollback guidance) — added Rollback section to release command
- **#16** (no pre-existing tag check) — added `git tag -l` check
- **#17** (no `uv sync` after `uv lock`) — added `uv lock && uv sync`
- **#19** (licence URL check) — covered by `test_licence_attribution` in `test_docs_accuracy.py`
- **#20** (live tests excluded by default) — already handled by `conftest.py` `--live` flag

---

## Remaining Issues

### 10. Live smoke test should use a permanent test file

The release command still uses the `/generating-smoke-tests` skill to create a one-off script. A permanent `tests/test_live_smoke.py` with `pytestmark = pytest.mark.live` would be more reliable and auditable.

**Current state:** Step is marked optional; `tests/test_arcgis_smoke.py` exists but only covers ArcGIS. A comprehensive multi-tool live test file is missing.

**Recommendation:** Create `tests/test_live_smoke.py` covering search → metadata → download → query → cache, marked `@pytest.mark.live`. Replace the skill invocation in the release command with `uv run pytest -m live`.

### 18. No `--dry-run` mode

No way to run all validation steps (preflight, tests, build) without committing, pushing, or publishing.

**Recommendation:** Document that `uv run pytest && rm -rf dist && uv build` is the dry-run equivalent, or add a note in the release command.

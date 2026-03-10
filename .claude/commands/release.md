---
name: release
description: Test, bump version, commit, push, and publish to PyPI
argument-hint: "[patch|minor|major] [optional: commit message]"
---

# Release

Bump the version, run tests, commit, push, and publish to PyPI.

The first argument is the bump type: `patch` (default), `minor`, or `major`.
Everything after the bump type is used as the commit message body. If no message is provided, generate one from the staged and unstaged changes.

## Steps

### 1. Preflight checks

Verify the working tree is ready for release. Stop immediately if any check fails.

```bash
# Must be on main
[ "$(git branch --show-current)" = "main" ] || { echo "ERROR: not on main"; exit 1; }

# Working tree must be clean (no staged or unstaged changes)
git diff --quiet && git diff --cached --quiet || { echo "ERROR: dirty working tree"; exit 1; }
```

### 2. Determine version bump

Parse `$ARGUMENTS` for the bump type. Default to `patch` if not specified or if the first word isn't patch/minor/major.

Read the current version from `pyproject.toml` and compute the new version:
- `patch`: 0.1.7 → 0.1.8
- `minor`: 0.1.7 → 0.2.0
- `major`: 0.1.7 → 1.0.0

Verify the tag `v{new_version}` does not already exist:

```bash
git tag -l "v{new_version}" | grep -q . && { echo "ERROR: tag v{new_version} already exists"; exit 1; }
```

### 3. Run tests

```bash
uv run pytest
```

Stop immediately if any tests fail. This covers:
- All unit tests
- Doc-accuracy checks (tool/resource counts, licence attribution, version consistency)

### 4. Live smoke test (optional)

```bash
uv run pytest -m live --live -v
```

This runs `tests/test_live_smoke.py` and `tests/test_arcgis_smoke.py` against real government APIs. Stop if any assertion fails.

Skip if the APIs are down or you're releasing a docs-only change.

### 5. Build

Build early so a failure doesn't leave a tagged commit with no PyPI release.

```bash
rm -rf dist && uv build
```

### 6. Bump version and update changelog

Edit `pyproject.toml` to set the new version. Then update the lockfile:

```bash
uv lock && uv sync
```

Add a new entry to `CHANGELOG.md` at the top (below the header). Follow the existing format:

```markdown
## [{new_version}] - {YYYY-MM-DD}

### Added / Changed / Fixed / Removed
- {description of changes}
```

Derive the changelog entry from the commit message and the actual changes being released.

### 7. Commit, tag, and push

**Ask for confirmation before committing.**

Show the user:
- The version bump (old → new)
- The changelog entry
- All files that will be staged
- The proposed commit message: `Release {new_version}: {message}`

Wait for user approval. Then:
- Stage `pyproject.toml`, `uv.lock`, `CHANGELOG.md`, and any other changed files relevant to the release
- Do NOT stage `.env`, credentials, or cache files
- Commit with the approved message
- Tag the commit: `git tag v{new_version}`

**Ask for confirmation before pushing.**

Verify HEAD is on main, then push:

```bash
[ "$(git branch --show-current)" = "main" ] || { echo "ERROR: not on main"; exit 1; }
git push origin main --tags
```

### 8. Publish

**Ask for confirmation before publishing.**

Show the user the version being published. Then:

```bash
uv publish
```

`uv publish` reads credentials from `~/.pypirc` natively. Do NOT pass tokens via command-line arguments (they leak into the process list).

### 9. Verify

Print the PyPI URL for manual verification:

```
https://pypi.org/project/ontario-data-mcp/{new_version}/
```

PyPI CDN propagation takes seconds to minutes — do not run an automated install check here; it will flake.

## Dry run

To validate everything without committing, pushing, or publishing:

```bash
uv run pytest && uv run pytest -m live --live -v && rm -rf dist && uv build
```

This runs all unit tests, live smoke tests, and verifies the build succeeds.

## Rollback

If publish fails after push:
1. **Same version:** PyPI publishes are immutable. You cannot re-publish. Bump to the next patch, fix, and release again.
2. **Delete the remote tag:** `git push origin :refs/tags/v{version}` and `git tag -d v{version}`
3. **Revert the commit:** `git revert HEAD && git push origin main`

## Success Criteria

- [ ] On main with clean working tree
- [ ] Tag `v{new_version}` does not pre-exist
- [ ] All unit tests pass (includes doc-accuracy and licence checks)
- [ ] `uv build` succeeds
- [ ] Version bumped in pyproject.toml, uv.lock, and CHANGELOG.md
- [ ] User confirmed commit → committed and tagged `v{new_version}`
- [ ] User confirmed push → pushed to origin with tags
- [ ] User confirmed publish → published to PyPI via `uv publish`

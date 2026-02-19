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

### 1. Clean dist

```bash
rm -rf dist
```

Always start fresh.

### 2. Determine version bump

Parse `$ARGUMENTS` for the bump type. Default to `patch` if not specified or if the first word isn't patch/minor/major.

Read the current version from `pyproject.toml` and compute the new version:
- `patch`: 0.1.5 → 0.1.6
- `minor`: 0.1.5 → 0.2.0
- `major`: 0.1.5 → 1.0.0

### 3. Run unit tests

```bash
uv run pytest
```

Stop immediately if any tests fail. Do not proceed.

### 4. Smoke test: server startup

Start the server and verify it initializes without errors:

```bash
uv run ontario-data-mcp &
SERVER_PID=$!
sleep 3
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null
```

Verify the output contains "Starting MCP server". If it shows an error (other than the kill signal), stop and report.

### 5. Smoke test: live multi-tool test

Use the `/generating-smoke-tests` skill to generate, run, and clean up a live smoke test. Stop if any assertion fails.

### 6. Licence compliance check

Read the `PORTALS` dict from `src/ontario_data/portals.py` and collect every portal's `licence_name`. For each licence name, verify that the exact string appears in both `README.md` and `site/index.html`.

Stop if any attribution is missing. Each data portal's licence requires its own attribution statement — omitting one violates the licence terms.

### 7. Bump version and update changelog

Edit `pyproject.toml` to set the new version. Then run `uv lock` to update the lockfile.

Add a new entry to `CHANGELOG.md` at the top (below the header). Follow the existing format:

```markdown
## [{new_version}] - {YYYY-MM-DD}

### Added / Changed / Fixed / Removed
- {description of changes}
```

Derive the changelog entry from the commit message and the actual changes being released.

### 8. Commit, tag, and push

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

Then push to origin with tags:

```bash
git push origin main --tags
```

### 9. Build and publish

**Ask for confirmation before publishing.**

Show the user the version being published. Then:

```bash
uv build
uv publish --token "$(grep -A2 '\[pypi\]' ~/.pypirc | grep password | sed 's/password = //')"
```

### 10. Verify

Check the package is available on PyPI:

```bash
uv pip install --dry-run ontario-data-mcp=={new_version} --index-url https://pypi.org/simple/
```

Report the published version and PyPI URL: https://pypi.org/project/ontario-data-mcp/{new_version}/

## Success Criteria

- [ ] dist/ cleaned
- [ ] All unit tests pass
- [ ] Server starts without errors
- [ ] Live multi-tool smoke test passes (search, metadata, download, query, cache)
- [ ] All portal licence attributions present in README.md and site/index.html
- [ ] Version bumped in pyproject.toml, uv.lock, and CHANGELOG.md
- [ ] User confirmed commit → committed and tagged `v{new_version}`
- [ ] User confirmed push → pushed to origin with tags
- [ ] User confirmed publish → published to PyPI
- [ ] Verified on PyPI

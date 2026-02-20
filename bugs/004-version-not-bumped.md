# BUG-004: Version still reads 0.1.6 despite unreleased changes

**Severity:** Low — confusing, not functional
**Status:** Identified

---

## Description

`pyproject.toml` still has `version = "0.1.6"` but HEAD has 20+ commits beyond the `v0.1.6` tag. The server's initialize response reports `"version": "0.1.6"`, making it impossible to distinguish the released version from the development version.

This makes debugging harder — a user running the published 0.1.6 package (which works) and a user running HEAD (which has the tools-not-visible bug) both see the same version string.

## Recommendation

Bump to `0.1.7-dev` or `0.2.0-dev` on the development branch.

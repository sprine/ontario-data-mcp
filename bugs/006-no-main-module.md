# BUG-006: No `__main__.py` — `python -m ontario_data` doesn't work

**Severity:** Low — alternative launch methods exist
**Status:** Identified

---

## Description

Running `python -m ontario_data` fails with:
```
No module named ontario_data.__main__; 'ontario_data' is a package and cannot be directly executed
```

This is a minor gap — the `ontario-data-mcp` entry point works, but `python -m` is a common way to run packages and would be useful for debugging.

## Recommendation

Add `src/ontario_data/__main__.py`:
```python
from ontario_data.server import main
main()
```

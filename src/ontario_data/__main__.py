"""Entry point for both MCP server and CLI cache commands.

CLI cache commands (``ontario-data-mcp cache …``) bypass the heavy
server module entirely, avoiding ~1 s of unnecessary startup.
"""
from __future__ import annotations

import sys


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "cache":
        from ontario_data.cli import run

        run(sys.argv[2:])
    else:
        from ontario_data.server import mcp

        mcp.run()


if __name__ == "__main__":
    main()

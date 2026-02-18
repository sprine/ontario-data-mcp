"""CLI for inspecting and managing the local DuckDB cache.

Usage: ontario-data-mcp cache <subcommand>
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone

from ontario_data.cache import CacheManager
from ontario_data.staleness import get_staleness_info


def _muted(text: str) -> str:
    return f"\033[90m{text}\033[0m"


def _make_cache(quiet: bool = False) -> CacheManager:
    cache = CacheManager()
    cache.initialize()
    if not quiet:
        print(_muted(cache.db_path))
    return cache


def _human_size(nbytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(nbytes) < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} TB"


def _print_table(headers: list[str], rows: list[list[str]]) -> None:
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))
    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    print(fmt.format(*headers))
    print(fmt.format(*["-" * w for w in widths]))
    for row in rows:
        print(fmt.format(*[str(c) for c in row]))


def cmd_list(args: argparse.Namespace) -> None:
    cache = _make_cache(quiet=args.json)
    cached = cache.list_cached()

    if not cached:
        print("Cache is empty.")
        return

    if args.json:
        for item in cached:
            staleness = get_staleness_info(cache, item["resource_id"])
            item["is_stale"] = staleness["is_stale"] if staleness else None
        print(json.dumps(cached, indent=2, default=str))
        return

    headers = ["resource_id", "table_name", "rows", "size", "downloaded_at", "stale?"]
    rows = []
    for item in cached:
        staleness = get_staleness_info(cache, item["resource_id"])
        stale = "yes" if staleness and staleness["is_stale"] else "no"
        rows.append([
            item["resource_id"][:12] + "...",
            item["table_name"],
            item["row_count"] or 0,
            _human_size(item["size_bytes"] or 0),
            item["downloaded_at"][:19],
            stale,
        ])
    _print_table(headers, rows)
    print(f"\n{len(cached)} resource(s) cached.")


def cmd_stats(args: argparse.Namespace) -> None:
    cache = _make_cache(quiet=args.json)
    stats = cache.get_stats()

    if args.json:
        print(json.dumps(stats, indent=2, default=str))
        return

    print(f"Tables:      {stats['table_count']}")
    print(f"Total rows:  {stats['total_rows']:,}")
    print(f"Total size:  {_human_size(stats['total_size_bytes'])}")


def cmd_query(args: argparse.Namespace) -> None:
    cache = _make_cache()
    try:
        results = cache.query(args.sql)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    if not results:
        print("No results.")
        return

    headers = list(results[0].keys())
    rows = []
    for r in results:
        rows.append([str(v)[:60] for v in r.values()])
    _print_table(headers, rows)
    print(f"\n{len(results)} row(s).")


def cmd_remove(args: argparse.Namespace) -> None:
    cache = _make_cache()
    if not cache.is_cached(args.resource_id):
        print(f"Resource {args.resource_id} is not cached.", file=sys.stderr)
        sys.exit(1)
    table_name = cache.get_table_name(args.resource_id)
    cache.remove_resource(args.resource_id)
    print(f"Removed {args.resource_id} (table: {table_name}).")


def cmd_clear(args: argparse.Namespace) -> None:
    cache = _make_cache()
    cached = cache.list_cached()
    if not cached:
        print("Cache is already empty.")
        return

    if not args.yes:
        answer = input(f"Remove all {len(cached)} cached resource(s)? [y/N] ")
        if answer.lower() != "y":
            print("Aborted.")
            return

    cache.remove_all()
    print(f"Cleared {len(cached)} resource(s).")


def cmd_refresh(args: argparse.Namespace) -> None:
    cache = _make_cache()
    if not cache.is_cached(args.resource_id):
        print(f"Resource {args.resource_id} is not cached.", file=sys.stderr)
        sys.exit(1)

    meta = cache.get_resource_meta(args.resource_id)

    async def _do_refresh():
        from ontario_data.ckan_client import CKANClient
        from ontario_data.staleness import compute_expires_at
        from ontario_data.tools.retrieval import _download_resource_data

        ckan = CKANClient()
        try:
            print(f"Downloading {args.resource_id}...")
            df, resource, dataset = await _download_resource_data(ckan, args.resource_id)

            table_name = meta["table_name"]
            cache.store_resource(
                resource_id=args.resource_id,
                dataset_id=meta["dataset_id"] or "",
                table_name=table_name,
                df=df,
                source_url=resource.get("url", ""),
            )

            update_freq = dataset.get("update_frequency")
            expires_at = compute_expires_at(datetime.now(timezone.utc), update_freq)
            cache.update_expires_at(args.resource_id, expires_at)

            print(f"Refreshed {args.resource_id}: {len(df)} rows -> {table_name}")
        finally:
            await ckan.close()

    asyncio.run(_do_refresh())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ontario-data-mcp cache",
        description="Inspect and manage the local DuckDB cache.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_list = sub.add_parser("list", help="List all cached resources")
    p_list.add_argument("--json", action="store_true", help="Output as JSON")
    p_list.set_defaults(func=cmd_list)

    p_stats = sub.add_parser("stats", help="Show cache statistics")
    p_stats.add_argument("--json", action="store_true", help="Output as JSON")
    p_stats.set_defaults(func=cmd_stats)

    p_query = sub.add_parser("query", help="Run read-only SQL against the cache")
    p_query.add_argument("sql", help="SQL query to execute")
    p_query.set_defaults(func=cmd_query)

    p_remove = sub.add_parser("remove", help="Remove a cached resource")
    p_remove.add_argument("resource_id", help="Resource ID to remove")
    p_remove.set_defaults(func=cmd_remove)

    p_clear = sub.add_parser("clear", help="Remove all cached resources")
    p_clear.add_argument("--yes", "-y", action="store_true", help="Skip confirmation")
    p_clear.set_defaults(func=cmd_clear)

    p_refresh = sub.add_parser("refresh", help="Re-download a cached resource")
    p_refresh.add_argument("resource_id", help="Resource ID to refresh")
    p_refresh.set_defaults(func=cmd_refresh)

    return parser


def run(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    run()

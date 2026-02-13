from fastmcp import FastMCP

mcp = FastMCP(
    "Ontario Data Catalogue",
    instructions="Search, download, cache, and analyze datasets from Ontario's Open Data Catalogue (data.ontario.ca).",
    version="0.1.0",
)


def main():
    mcp.run()


if __name__ == "__main__":
    main()

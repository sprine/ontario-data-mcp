from __future__ import annotations

from fastmcp import Context
from fastmcp.prompts import Message

from ontario_data.server import mcp


@mcp.prompt
async def explore_topic(topic: str) -> list[Message]:
    """Guided exploration of a topic in Ontario's open data.

    Searches for datasets, summarizes what's available, and suggests deep dives.
    """
    return [
        Message(
            role="user",
            content=(
                f"I want to explore Ontario open data about: {topic}\n\n"
                "Please:\n"
                "1. Use search_datasets to find relevant datasets\n"
                "2. Summarize the top results — what data is available, from which ministries\n"
                "3. For the most interesting datasets, use get_dataset_info to get details\n"
                "4. Suggest which datasets to download and analyze, and what questions they could answer\n"
                "5. If any have datastore_active resources, preview a few rows"
            ),
        ),
    ]


@mcp.prompt
async def data_investigation(dataset_id: str) -> list[Message]:
    """Deep investigation of a specific dataset: schema, quality, statistics, insights."""
    return [
        Message(
            role="user",
            content=(
                f"Investigate this Ontario dataset thoroughly: {dataset_id}\n\n"
                "Please follow this workflow:\n"
                "1. get_dataset_info — understand what this dataset contains\n"
                "2. list_resources — see all available files\n"
                "3. For the primary CSV/data resource:\n"
                "   a. get_resource_schema — understand the columns\n"
                "   b. download_resource — cache it locally\n"
                "   c. check_data_quality — assess completeness and consistency\n"
                "   d. profile_dataset — full statistical profile\n"
                "   e. summarize — key statistics\n"
                "4. Provide insights: What stories does this data tell? What's surprising?\n"
                "5. Suggest follow-up analyses or related datasets"
            ),
        ),
    ]


@mcp.prompt
async def compare_data(dataset_ids: str) -> list[Message]:
    """Side-by-side analysis of multiple datasets (comma-separated IDs)."""
    ids = [d.strip() for d in dataset_ids.split(",")]
    return [
        Message(
            role="user",
            content=(
                f"Compare these Ontario datasets side by side: {', '.join(ids)}\n\n"
                "Please:\n"
                "1. compare_datasets — metadata comparison\n"
                "2. For each dataset, download the primary resource\n"
                "3. profile_dataset on each — compare structure, size, quality\n"
                "4. If they share common columns, look for relationships\n"
                "5. Summarize: How do these datasets complement each other? Can they be joined?"
            ),
        ),
    ]

"""Protocol definitions for portal clients.

Both CKANClient and ArcGISHubClient implement the PortalClient protocol,
ensuring a consistent interface across different portal backends.
"""
from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class PortalClient(Protocol):
    """Common interface for data portal clients (CKAN, ArcGIS Hub)."""

    async def close(self) -> None: ...

    async def package_search(
        self,
        query: str = "",
        rows: int = 10,
        **kwargs: Any,
    ) -> dict[str, Any]: ...

    async def package_show(self, id: str) -> dict[str, Any]: ...

    async def resource_show(self, id: str) -> dict[str, Any]: ...

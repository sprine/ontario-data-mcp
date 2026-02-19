from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class PortalType(StrEnum):
    CKAN = "ckan"
    ARCGIS_HUB = "arcgis_hub"


@dataclass(frozen=True, slots=True)
class PortalConfig:
    name: str
    base_url: str
    portal_type: PortalType
    description: str


PORTALS: dict[str, PortalConfig] = {
    "ontario": PortalConfig(
        name="Ontario Open Data",
        base_url="https://data.ontario.ca",
        portal_type=PortalType.CKAN,
        description="Province of Ontario Open Data Catalogue (~5,700 datasets)",
    ),
    "toronto": PortalConfig(
        name="Toronto Open Data",
        base_url="https://ckan0.cf.opendata.inter.prod-toronto.ca",
        portal_type=PortalType.CKAN,
        description="City of Toronto Open Data Portal (~533 datasets)",
    ),
    "ottawa": PortalConfig(
        name="Ottawa Open Data",
        base_url="https://open.ottawa.ca",
        portal_type=PortalType.ARCGIS_HUB,
        description="City of Ottawa Open Data (~665 datasets)",
    ),
}

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
    licence_name: str
    licence_url: str


PORTALS: dict[str, PortalConfig] = {
    "ontario": PortalConfig(
        name="Ontario Open Data",
        base_url="https://data.ontario.ca",
        portal_type=PortalType.CKAN,
        description="Province of Ontario Open Data Catalogue (~5,700 datasets)",
        licence_name="Open Government Licence – Ontario",
        licence_url="https://www.ontario.ca/page/open-government-licence-ontario",
    ),
    "toronto": PortalConfig(
        name="Toronto Open Data",
        base_url="https://ckan0.cf.opendata.inter.prod-toronto.ca",
        portal_type=PortalType.CKAN,
        description="City of Toronto Open Data Portal (~533 datasets)",
        licence_name="Open Government Licence – Toronto",
        licence_url="https://open.toronto.ca/open-data-licence/",
    ),
    "ottawa": PortalConfig(
        name="Ottawa Open Data",
        base_url="https://open.ottawa.ca",
        portal_type=PortalType.ARCGIS_HUB,
        description="City of Ottawa Open Data (~665 datasets)",
        licence_name="Open Government Licence – City of Ottawa",
        licence_url="https://open.ottawa.ca/pages/open-data-licence",
    ),
}

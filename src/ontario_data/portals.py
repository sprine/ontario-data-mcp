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
    owner_filter: str | None = None  # ArcGIS Hub: filter search to this owner username


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
    # Waterloo Region — three separate portals sharing the same ArcGIS Hub federation.
    # owner_filter scopes each portal's search to its own org, preventing triple duplicates.
    "waterloo": PortalConfig(
        name="City of Waterloo Open Data",
        base_url="https://data.waterloo.ca",
        portal_type=PortalType.ARCGIS_HUB,
        description="City of Waterloo Open Data (~129 datasets)",
        licence_name="City of Waterloo Open Data Licence",
        licence_url="https://data.waterloo.ca/pages/open-data-licence",
        owner_filter="OpenData_Waterloo",
    ),
    "kitchener": PortalConfig(
        name="City of Kitchener Open Data",
        base_url="https://open-kitchenergis.opendata.arcgis.com",
        portal_type=PortalType.ARCGIS_HUB,
        description="City of Kitchener GeoHub (~219 datasets)",
        licence_name="Open Government Licence - The Corporation of the City of Kitchener",
        licence_url="https://www.kitchener.ca/council-and-city-administration/data-and-maps/open-data-licence/",
        owner_filter="KitchenerGIS",
    ),
    "region-waterloo": PortalConfig(
        name="Region of Waterloo Open Data",
        base_url="https://rowopendata-rmw.opendata.arcgis.com",
        portal_type=PortalType.ARCGIS_HUB,
        description="Region of Waterloo Open Data (~125 datasets)",
        licence_name="Region of Waterloo Open Data Licence v.2.0",
        licence_url="https://www.regionofwaterloo.ca/en/regional-government/open-data.aspx",
        owner_filter="RegionOfWaterloo",
    ),
}

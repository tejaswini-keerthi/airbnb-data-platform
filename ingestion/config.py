"""
Configuration for Airbnb data ingestion.
Defines target cities, snapshot URLs, and S3 paths.
"""

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class CityConfig:
    name: str
    country: str
    currency: str
    inside_airbnb_id: str    # city slug in URL
    state: Optional[str]      # state/region in URL (e.g. 'ny', 'ile-de-france')
    url_country: str          # country slug in URL (e.g. 'united-states', 'france')


# Cities we're ingesting — chosen for geographic + currency diversity
CITIES: List[CityConfig] = [
    CityConfig(
        name="new_york",
        country="usa",
        currency="USD",
        inside_airbnb_id="new-york-city",
        state="ny",
        url_country="united-states",
    ),
    CityConfig(
        name="london",
        country="uk",
        currency="GBP",
        inside_airbnb_id="london",
        state="england",
        url_country="united-kingdom",
    ),
    CityConfig(
        name="paris",
        country="france",
        currency="EUR",
        inside_airbnb_id="paris",
        state="ile-de-france",
        url_country="france",
    ),
    CityConfig(
        name="amsterdam",
        country="netherlands",
        currency="EUR",
        inside_airbnb_id="amsterdam",
        state="north-holland",
        url_country="the-netherlands",
    ),
    CityConfig(
        name="sydney",
        country="australia",
        currency="AUD",
        inside_airbnb_id="sydney",
        state="nsw",
        url_country="australia",
    ),
]

# Inside Airbnb base URL pattern
INSIDE_AIRBNB_BASE_URL = "https://data.insideairbnb.com"

# Files we download per city per snapshot
SNAPSHOT_FILES = ["listings.csv.gz", "calendar.csv.gz", "reviews.csv.gz"]

# S3 path patterns
S3_BRONZE_PREFIX = "raw"
S3_SILVER_PREFIX = "cleaned"
S3_GOLD_PREFIX = "star_schema"

# Local temp directory for downloads before S3 upload
LOCAL_TEMP_DIR = "/tmp/airbnb_downloads"
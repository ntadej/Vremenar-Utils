"""ARSO 48-hour measurements parsing."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from httpx import AsyncClient, codes
from xmltodict import parse  # type: ignore[import-untyped]

from . import TIMEOUT

if TYPE_CHECKING:
    from vremenar_utils.cli.logging import Logger

ARSO_DATA_ENDPOINT = "https://meteo.arso.gov.si/uploads/probase/www/observ/surface/text/sl/recent/observationAms_{0}_history.xml"


class MeteoSIWebMetParser:
    """ARSO MeteoSI WebMet parser."""

    def __init__(self, logger: Logger, stations: dict[str, str]) -> None:
        """Initialize MeteoSI WebMet parser."""
        self.logger: Logger = logger
        self.stations: dict[str, str] = stations

    async def get_data(self, station_id: str) -> list[dict[str, str | int | float]]:
        """Retrieve new alerts."""
        endpoint = ARSO_DATA_ENDPOINT.format(self.stations[station_id])
        async with AsyncClient() as client:
            response = await client.get(endpoint, timeout=TIMEOUT)
        # can be invalid
        if response.status_code != codes.OK:  # pragma: no cover
            return []

        result = response.text

        # Parse the XML response for the alert feed and loop over the entries
        feed_data = parse(result)
        data = feed_data.get("data")
        if not data:  # pragma: no cover
            return []

        entries: list[dict[str, str | int | float]] = []
        met_data = data.get("metData")
        for entry in met_data:
            valid_date = datetime.strptime(
                entry.get("tsValid_issued_RFC822"),
                "%d %b %Y %H:%M:%S %z",
            )
            t = entry.get("t")
            if t is None:
                continue

            entries.append(
                {
                    "source": f"arso:48h:{station_id}",
                    "station_id": station_id,
                    "timestamp": f"{int(valid_date.timestamp())}000",
                    "temperature": float(t),
                },
            )

        return sorted(entries, key=lambda x: x["timestamp"])

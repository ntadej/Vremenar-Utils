"""DWD current weather utils."""
from brightsky.parsers import CurrentObservationsParser  # type: ignore
from csv import reader, DictReader
from httpx import AsyncClient
from io import BytesIO, TextIOWrapper
from pkgutil import get_data
from tempfile import NamedTemporaryFile
from typing import Any, Generator, IO

from ..cli.logging import Logger
from ..database.redis import redis

from .database import BatchedCurrentWeather

DwdRecord = dict[str, Any]
DwdGenerator = Generator[DwdRecord, None, None]


class CurrentWeatherParser(CurrentObservationsParser):  # type: ignore
    """Custom current weather parser for low memory."""

    def parse(
        self,
        lat: None = None,
        lon: None = None,
        height: None = None,
        station_name: None = None,
    ) -> DwdGenerator:
        """Parse current weather."""
        with open(self.path) as f:
            reader = DictReader(f, delimiter=';')
            wmo_station_id = next(reader)[self.DATE_COLUMN].rstrip('_')
            # Skip row with German header titles
            next(reader)
            for row in reader:
                yield {
                    'station_id': wmo_station_id,
                    **self.parse_row(row),
                }
                break  # only parse first row for now


def current_stations() -> list[str]:
    """Get a list of supported DWD stations."""
    stations: list[str] = []
    data = get_data('vremenar_utils', 'data/stations/DWD.current.csv')
    if data:
        bytes_io = BytesIO(data)
        with TextIOWrapper(bytes_io, encoding='utf-8') as csvfile:
            csv = reader(csvfile)
            for row in csv:
                stations.append(row[0])
    return stations


async def download_current_weather(
    logger: Logger, url: str, temporary_file: IO[bytes]
) -> None:
    """Download the mosmix data."""
    logger.info(f'Downloading current weather data from {url} ...')
    logger.info(f'Temporary file: {temporary_file.name}')
    client = AsyncClient()
    async with client.stream('GET', url) as r:
        async for chunk in r.aiter_raw():
            temporary_file.write(chunk)
    temporary_file.flush()
    await client.aclose()


async def current_weather(logger: Logger) -> None:
    """Cache DWD current weather data."""
    stations: list[str] = current_stations()

    async with redis.client() as db:
        async with BatchedCurrentWeather(db) as batch:
            for station_id in stations:
                if len(station_id) == 4:
                    station_id += '_'
                url = (
                    'https://opendata.dwd.de/weather/weather_reports/poi/'
                    f'{station_id}-BEOB.csv'
                )

                temporary_file = NamedTemporaryFile(
                    suffix='.csv', prefix=f'DWD_CURRENT_{station_id}'
                )
                await download_current_weather(logger, url, temporary_file)

                try:
                    parser = CurrentWeatherParser(path=temporary_file.name)
                    for record in parser.parse():
                        # update timestamps
                        record[
                            'timestamp'
                        ] = f"{int(record['timestamp'].timestamp())}000"
                        # store
                        await batch.add(record)
                finally:
                    temporary_file.close()

                logger.info(
                    'Done getting current weather data for station %s', station_id
                )

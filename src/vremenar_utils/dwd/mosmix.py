"""DWD MOSMIX utils."""
import re
from brightsky.parsers import Parser, wmo_id_to_dwd  # type: ignore
from csv import reader
from datetime import datetime, timedelta, timezone
from dateutil import parser
from httpx import stream
from logging import Logger
from lxml.etree import iterparse, Element, QName  # type: ignore
from typing import Any, Generator, IO, Optional, cast
from zipfile import ZipFile

DwdRecord = dict[str, Any]
DwdGenerator = Generator[DwdRecord, None, None]

DWD_OPEN_DATA: str = 'https://opendata.dwd.de'
DWD_MOSMIX_URL: str = (
    f'{DWD_OPEN_DATA}/weather/local_forecasts/mos/MOSMIX_S/'
    'all_stations/kml/MOSMIX_S_LATEST_240.kmz'
)
NS = {
    'dwd': f'{DWD_OPEN_DATA}/weather/lib/pointforecast_dwd_extension_V1_0.xsd',
    'kml': 'http://www.opengis.net/kml/2.2',
}


class MOSMIXParserFast(Parser):  # type: ignore
    """Custom MOSMIX parser for low memory."""

    ELEMENTS = {
        'DD': 'wind_direction',
        'FF': 'wind_speed',
        'FX1': 'wind_gust_speed',
        'N': 'cloud_cover',
        'PPPP': 'pressure_msl',
        'RR1c': 'precipitation',
        'SunD1': 'sunshine',
        'Td': 'dew_point',
        'TTT': 'temperature',
        'VV': 'visibility',
        'ww': 'condition',
    }

    def parse(
        self, station_ids: list[str], min_entry: int, max_entry: int
    ) -> DwdGenerator:
        """Parse the file."""
        self.logger.info('Parsing %s', self.path)

        with ZipFile(self.path) as zip:
            with zip.open(zip.namelist()[0]) as file:
                timestamps = []
                accepted_timestamps = []
                placemark = 0
                source = ''
                for _, elem in iterparse(file):
                    tag = QName(elem.tag).localname

                    if tag == 'ProductID':
                        source += elem.text + ':'
                        self._clear_element(elem)
                    elif tag == 'IssueTime':
                        source += elem.text
                        self._clear_element(elem)
                    elif tag == 'ForecastTimeSteps':
                        timestamps = [
                            parser.parse(r.text)
                            for r in elem.findall('dwd:TimeStep', namespaces=NS)
                        ]
                        timestamps = [
                            t.replace(tzinfo=timezone.utc) for t in timestamps
                        ]
                        accepted_timestamps = self._filter_timestamps(timestamps)
                        self._clear_element(elem)

                        self.logger.info(
                            'Got %d timestamps for source %s'
                            % (len(timestamps), source)
                        )
                        self.logger.info(
                            'Using %d timestamps' % (len(accepted_timestamps),)
                        )
                    elif tag == 'Placemark':
                        records = None
                        if placemark >= min_entry and (
                            max_entry == 0 or placemark < max_entry
                        ):
                            records = self._parse_station(
                                elem,
                                station_ids,
                                timestamps,
                                accepted_timestamps,
                                source,
                            )
                        self._clear_element(elem)
                        if max_entry > 0 and placemark >= max_entry:
                            break
                        if records:
                            self.logger.info(f'Processed placemark #{placemark+1}')
                            placemark += 1
                            yield from self._sanitize_records(records)
                        else:
                            continue

    def stations(self) -> DwdGenerator:
        """Parse the file."""
        self.logger.info('Parsing %s', self.path)

        with ZipFile(self.path) as zip:
            with zip.open(zip.namelist()[0]) as file:
                for _, elem in iterparse(file):
                    tag = QName(elem.tag).localname

                    if tag in ['ProductID', 'IssueTime', 'ForecastTimeSteps']:
                        self._clear_element(elem)
                    elif tag == 'Placemark':
                        records = self._parse_station(elem, [], [], [])
                        self._clear_element(elem)
                        if records:
                            yield from records
                        else:
                            continue

    @staticmethod
    def _clear_element(elem: Element) -> None:
        elem.clear()
        # Also eliminate now-empty references from the root node to elem
        for ancestor in elem.xpath('ancestor-or-self::*'):
            while ancestor.getprevious() is not None:
                del ancestor.getparent()[0]

    @staticmethod
    def _filter_timestamps(timestamps: list[datetime]) -> list[datetime]:
        accepted = []
        now = datetime.now(tz=timezone.utc)
        now = now.replace(minute=0, second=0, microsecond=0)
        if now >= timestamps[0] and now <= timestamps[-1]:
            accepted.append(now)

        daily = now + timedelta(hours=48 - now.hour)

        # 2 days hourly
        while now < daily:
            now += timedelta(hours=1)
            if now >= timestamps[0] and now <= timestamps[-1]:
                accepted.append(now)

        # 7 days
        for i in range(28):
            time = daily + timedelta(hours=i * 6)
            if time >= timestamps[0] and time <= timestamps[-1]:
                accepted.append(time)

        return accepted

    def _parse_station(
        self,
        station_elem: Element,
        station_ids: list[str],
        timestamps: list[datetime],
        accepted_timestamps: list[datetime],
        source: Optional[str] = '',
    ) -> DwdGenerator:
        wmo_station_id = station_elem.find('./kml:name', namespaces=NS).text
        if station_ids and wmo_station_id not in station_ids:
            return cast(DwdGenerator, [])

        station_name = station_elem.find('./kml:description', namespaces=NS).text
        try:
            lon, lat, altitude = station_elem.find(
                './kml:Point/kml:coordinates', namespaces=NS
            ).text.split(',')
        except AttributeError:
            self.logger.warning(
                "Ignoring station without coordinates, WMO ID '%s', name '%s'",
                wmo_station_id,
                station_name,
            )
            return cast(DwdGenerator, [])

        base_record = {
            'source': source,
            'wmo_station_id': wmo_station_id,
        }

        if timestamps:
            records: dict[str, Any] = {'timestamp': timestamps}
            for element, column in self.ELEMENTS.items():
                values_str = station_elem.find(
                    f'./*/dwd:Forecast[@dwd:elementName="{element}"]/dwd:value',
                    namespaces=NS,
                ).text
                converter = getattr(self, f'parse_{column}', float)
                records[column] = [
                    None if row[0] == '-' else converter(row[0])
                    for row in reader(
                        re.sub(r'\s+', '\n', values_str.strip()).splitlines()
                    )
                ]
                assert len(records[column]) == len(timestamps)

            # Turn dict of lists into list of dicts
            return (
                {**base_record, **dict(zip(records, row))}
                for row in zip(*records.values())
                if row[0] in accepted_timestamps
            )

        else:
            dwd_station_id = wmo_id_to_dwd(wmo_station_id)
            base_record.update(
                {
                    'lat': float(lat),
                    'lon': float(lon),
                    'altitude': float(altitude),
                    'dwd_station_id': dwd_station_id,
                    'station_name': station_name,
                }
            )
            return cast(DwdGenerator, [base_record])

    def _sanitize_records(self, records: DwdGenerator) -> DwdGenerator:
        for r in records:
            if r['precipitation'] and r['precipitation'] < 0:
                self.logger.warning('Ignoring negative precipitation value: %s', r)
                r['precipitation'] = None
            if r['wind_direction'] and r['wind_direction'] > 360:
                self.logger.warning('Fixing out-of-bounds wind direction: %s', r)
                r['wind_direction'] -= 360
            yield r


def download(logger: Logger, temporary_file: IO[bytes]) -> None:
    """Download the mosmix data."""
    logger.info(f'Downloading MOSMIX data from {DWD_MOSMIX_URL} ...')
    logger.info(f'Temporary file: {temporary_file.name}')
    with stream('GET', DWD_MOSMIX_URL) as r:
        for chunk in r.iter_raw():
            temporary_file.write(chunk)
    temporary_file.flush()
    logger.info('Done!')

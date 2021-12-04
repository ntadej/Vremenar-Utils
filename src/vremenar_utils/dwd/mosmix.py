"""DWD MOSMIX utils."""
import re
from brightsky.parsers import Parser  # type: ignore
from csv import reader
from datetime import datetime, timedelta, timezone
from dateutil import parser
from deta import Deta  # type: ignore
from httpx import stream
from json import dumps
from lxml.etree import iterparse, Element, QName  # type: ignore
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Generator, IO, List, Optional, TextIO, cast
from zipfile import ZipFile

from ..database.utils import BatchedPut

DwdRecord = Dict[str, Any]
DwdGenerator = Generator[DwdRecord, None, None]

DWD_OPEN_DATA: str = 'https://opendata.dwd.de'
DWD_MOSMIX_URL: str = (
    f'{DWD_OPEN_DATA}/weather/local_forecasts/mos/MOSMIX_S/'
    'all_stations/kml/MOSMIX_S_LATEST_240.kmz'
)
DWD_CACHE_DIR: Path = Path.cwd() / '.cache/dwd'
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

    def parse(self, min_entry: int, max_entry: int) -> DwdGenerator:
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

                        print(
                            'Got %d timestamps for source %s'
                            % (len(timestamps), source)
                        )
                        print('Using %d timestamps' % (len(accepted_timestamps),))
                    elif tag == 'Placemark':
                        records = None
                        if placemark >= min_entry and (
                            max_entry == 0 or placemark < max_entry
                        ):
                            print(f'Processing placemark #{placemark+1}')
                            records = self._parse_station(
                                elem, timestamps, accepted_timestamps, source
                            )
                        self._clear_element(elem)
                        if max_entry > 0 and placemark >= max_entry:
                            break
                        placemark += 1
                        if records:
                            yield from self._sanitize_records(records)
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
    def _filter_timestamps(timestamps: List[datetime]) -> List[datetime]:
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
        timestamps: List[datetime],
        accepted_timestamps: List[datetime],
        source: str,
    ) -> DwdGenerator:
        wmo_station_id = station_elem.find('./kml:name', namespaces=NS).text
        station_name = station_elem.find('./kml:description', namespaces=NS).text
        try:
            lon, lat, height = station_elem.find(
                './kml:Point/kml:coordinates', namespaces=NS
            ).text.split(',')
        except AttributeError:
            self.logger.warning(
                "Ignoring station without coordinates, WMO ID '%s', name '%s'",
                wmo_station_id,
                station_name,
            )
            return cast(DwdGenerator, [])
        records: Dict[str, Any] = {'timestamp': timestamps}
        for element, column in self.ELEMENTS.items():
            values_str = station_elem.find(
                f'./*/dwd:Forecast[@dwd:elementName="{element}"]/dwd:value',
                namespaces=NS,
            ).text
            converter = getattr(self, f'parse_{column}', float)
            records[column] = [
                None if row[0] == '-' else converter(row[0])
                for row in reader(re.sub(r'\s+', '\n', values_str.strip()).splitlines())
            ]
            assert len(records[column]) == len(timestamps)
        base_record = {
            'source': source,
            'wmo_station_id': wmo_station_id,
        }
        # Turn dict of lists into list of dicts
        return (
            {**base_record, **dict(zip(records, row))}
            for row in zip(*records.values())
            if row[0] in accepted_timestamps
        )

    def _sanitize_records(self, records: DwdGenerator) -> DwdGenerator:
        for r in records:
            if r['precipitation'] and r['precipitation'] < 0:
                self.logger.warning('Ignoring negative precipitation value: %s', r)
                r['precipitation'] = None
            if r['wind_direction'] and r['wind_direction'] > 360:
                self.logger.warning('Fixing out-of-bounds wind direction: %s', r)
                r['wind_direction'] -= 360
            yield r


def download_mosmix(temporary_file: IO[bytes]) -> None:
    """Download the mosmix data."""
    print(f'Downloading MOSMIX data from {DWD_MOSMIX_URL} ...')
    print(f'Temporary file: {temporary_file.name}')
    with stream('GET', DWD_MOSMIX_URL) as r:
        for chunk in r.iter_raw():
            temporary_file.write(chunk)
    print('Done!')


def output_name(date: datetime) -> str:
    """Get MOSMIX cache file name."""
    return date.strftime('MOSMIX:%Y-%m-%dT%H:%M:%S') + 'Z'


def open_file(source: str) -> TextIO:
    """Open cache file."""
    file = open(DWD_CACHE_DIR / f'{source}.json', 'w')
    print('[', file=file)
    return file


def close_file(file: TextIO) -> None:
    """Close cache file."""
    print(']', file=file)
    file.close()


def process_mosmix(
    disable_cache: Optional[bool] = False,
    disable_database: Optional[bool] = False,
    job: Optional[int] = 0,
) -> str:
    """Cache DWD MOSMIX data."""
    if not disable_cache:
        DWD_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    data: Dict[str, TextIO] = {}

    db = None
    if not disable_database:
        deta = Deta()
        db = deta.Base('dwd_mosmix')

    # setup batching if needed
    job_size: int = 250
    min_entry: int = 0
    max_entry: int = 0
    message: str = 'Processed all placemarks'
    if job and job > 0:
        min_entry = (job - 1) * job_size
        max_entry = job * job_size
        message = f'Processed placemarks from #{min_entry+1} to #{max_entry}'
        print(f'Processing placemarks from #{min_entry+1} to #{max_entry}')

    with BatchedPut(db) as batch, NamedTemporaryFile(
        suffix='.kmz', prefix='DWD_MOSMIX_'
    ) as temporary_file:
        download_mosmix(temporary_file)

        parser = MOSMIXParserFast(path=temporary_file.name, url=None)
        # parser.download()  # Not necessary if you supply a local path
        for record in parser.parse(min_entry, max_entry):
            source: str = output_name(record['timestamp'])
            record['timestamp'] = str(int(record['timestamp'].timestamp())) + '000'
            # write to the DB
            if not disable_database:
                key: str = f"{record['timestamp']}_{record['wmo_station_id']}"
                batch.put(record, key)
            # write to the local cache
            if not disable_cache:
                if source not in data:
                    data[source] = open_file(source)
                    data[source].write(dumps(record))
                else:
                    data[source].write(f',\n{dumps(record)}')
        # parser.cleanup()  # If you wish to delete any downloaded files

    if not disable_cache:
        for _, file in data.items():
            close_file(file)

    return message

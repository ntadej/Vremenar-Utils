"""MeteoAlarm steering code."""
from deta import Deta  # type: ignore

from ..database.utils import BatchedPut

from .common import AlertCountry, AlertInfo
from .parser import MeteoAlarmParser


def get_alerts(country: AlertCountry) -> None:
    """Get alerts for a specific country."""
    deta = Deta()
    db = deta.Base(f'{country.value}_meteoalarm_alerts')

    existing_alerts: list[AlertInfo] = []

    last_item = None
    total_count = 0
    while True:
        result = db.fetch(last=last_item)
        total_count += result.count
        for item in result.items:
            existing_alerts.append(AlertInfo.from_dict(item))
        if not result.last:
            break
        last_item = result.last

    print(f'Read {total_count} existing alerts from the database')
    print()

    parser = MeteoAlarmParser(country, existing_alerts)
    new_alerts = parser.get_new_alerts()

    with BatchedPut(db) as batch:
        for id, url in new_alerts:
            alert = parser.parse_cap(id, url)
            batch.put(alert.to_dict(), alert.id)

    print(f'Added {len(new_alerts)} new alerts')

    for id in parser.obsolete_alert_ids:
        db.delete(id)

    print(f'Removed {len(parser.obsolete_alert_ids)} obsolete alerts')

    last_item = None
    total_count = 0
    while True:
        result = db.fetch(last=last_item)
        total_count += result.count
        if not result.last:
            break
        last_item = result.last

    print()
    print(f'Total of {total_count} alerts are available for {country.value}')

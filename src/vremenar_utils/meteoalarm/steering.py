"""MeteoAlarm steering code."""
from deta import Deta  # type: ignore

from ..database.utils import BatchedPut

from .areas import load_meteoalarm_areas
from .common import AlertCountry, AlertInfo, AlertNotificationInfo
from .parser import MeteoAlarmParser


def get_alerts(country: AlertCountry) -> None:
    """Get alerts for a specific country."""
    deta = Deta()
    db_alerts = deta.Base(f'{country.value}_meteoalarm_alerts')
    db_notifications = deta.Base(f'{country.value}_meteoalarm_notifications')

    existing_alerts: list[AlertInfo] = []

    last_item = None
    total_count = 0
    while True:
        result = db_alerts.fetch(last=last_item)
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

    with BatchedPut(db_alerts, limit=5) as batch_alerts, BatchedPut(
        db_notifications
    ) as batch_notifications:
        for id, url in new_alerts:
            alert = parser.parse_cap(id, url)
            batch_alerts.put(alert.to_dict(), alert.id)
            batch_notifications.put(AlertNotificationInfo(alert.id).to_dict(), alert.id)

    print(f'Added {len(new_alerts)} new alerts')

    # remove expired
    obsolete = parser.obsolete_alert_ids
    for alert in existing_alerts:
        if alert.expires < parser.now:
            obsolete.add(alert.id)

    for id in obsolete:
        db_alerts.delete(id)
        db_notifications.delete(id)

    print(f'Removed {len(obsolete)} obsolete alerts')

    all_alerts: list[AlertInfo] = []
    last_item = None
    total_count = 0
    while True:
        result = db_alerts.fetch(last=last_item)
        total_count += result.count
        for item in result.items:
            all_alerts.append(AlertInfo.from_dict(item))
        if not result.last:
            break
        last_item = result.last

    print()
    print(f'Total of {total_count} alerts are available for {country.value}')

    # make area-alert mappings
    areas = load_meteoalarm_areas(country)
    areas_with_alerts: set[str] = set()
    area_mappings: dict[str, list[str]] = {area.code: [] for area in areas}
    for alert in all_alerts:
        for area in alert.areas:
            if alert.id not in area_mappings[area]:
                area_mappings[area].append(alert.id)
                areas_with_alerts.add(area)

    db_mappings = deta.Base(f'{country.value}_meteoalarm_areas')
    with BatchedPut(db_mappings) as batch:
        for area, alerts in area_mappings.items():
            batch.put({'alerts': alerts}, area)

    print()
    print(f'Areas with alerts: {len(areas_with_alerts)}')

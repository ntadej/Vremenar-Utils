"""MeteoAlarm steering code."""
from .areas import load_meteoalarm_areas
from .database import (
    delete_alert,
    get_alert_area_map,
    get_alert_ids,
    store_alert,
    store_alerts_for_area,
)
from .common import AlertCountry
from .parser import MeteoAlarmParser


async def get_alerts(country: AlertCountry) -> None:
    """Get alerts for a specific country."""
    existing_alerts: set[str] = await get_alert_ids(country)
    print(f'Read {len(existing_alerts)} existing alerts from the database')
    print()

    parser = MeteoAlarmParser(country, existing_alerts)
    new_alerts = parser.get_new_alerts()

    for id, url in new_alerts:
        alert = parser.parse_cap(id, url)
        if not alert:
            continue
        await store_alert(country, alert)

    print(f'Added {len(new_alerts)} new alerts')

    # TODO: remove expired

    for id in parser.obsolete_alert_ids:
        await delete_alert(country, id)

    print(f'Removed {len(parser.obsolete_alert_ids)} obsolete alerts')

    alert_areas: dict[str, set[str]] = await get_alert_area_map(country)

    print()
    print(f'Total of {len(alert_areas)} alerts are available for {country.value}')

    # make area-alert mappings
    areas_list = load_meteoalarm_areas(country)
    areas_with_alerts: set[str] = set()
    area_mappings: dict[str, set[str]] = {area.code: set() for area in areas_list}
    for alert_id, areas in alert_areas.items():
        for area in areas:
            area_mappings[area].add(alert_id)
            areas_with_alerts.add(area)

    for area, alerts in area_mappings.items():
        await store_alerts_for_area(country, area, alerts)

    print()
    print(f'Areas with alerts: {len(areas_with_alerts)}')

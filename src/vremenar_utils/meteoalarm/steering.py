"""MeteoAlarm steering code."""
from ..cli.common import CountryID
from ..cli.logging import Logger

from .areas import load_meteoalarm_areas
from .database import (
    delete_alert,
    get_alert_area_map,
    get_alert_ids,
    store_alert,
    store_alerts_for_area,
)
from .parser import MeteoAlarmParser


async def get_alerts(logger: Logger, country: CountryID) -> None:
    """Get alerts for a specific country."""
    existing_alerts: set[str] = await get_alert_ids(country)
    logger.info(f'Read {len(existing_alerts)} existing alerts from the database')

    parser = MeteoAlarmParser(logger, country, existing_alerts)
    new_alerts = await parser.get_new_alerts()

    for id, url in new_alerts:
        alert = await parser.parse_cap(id, url)
        if not alert:
            continue
        await store_alert(country, alert)

    logger.info(f'Added {len(new_alerts)} new alerts')

    for id in parser.obsolete_alert_ids:
        await delete_alert(country, id)

    logger.info(f'Removed {len(parser.obsolete_alert_ids)} obsolete alerts')

    alert_areas: dict[str, set[str]] = await get_alert_area_map(country)

    logger.info(f'Total of {len(alert_areas)} alerts are available for {country.value}')

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

    logger.info(f'Areas with alerts: {len(areas_with_alerts)}')

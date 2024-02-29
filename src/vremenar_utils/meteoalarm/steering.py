"""MeteoAlarm steering code."""

from vremenar_utils.cli.common import CountryID
from vremenar_utils.cli.logging import Logger, progress_bar

from .areas import build_meteoalarm_area_description_map, load_meteoalarm_areas
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
    logger.info("Read %d existing alerts from the database", len(existing_alerts))

    parser = MeteoAlarmParser(logger, country, existing_alerts)
    new_alerts = await parser.get_new_alerts()
    counter = 0

    areas_list = load_meteoalarm_areas(country)
    areas_desc_map = build_meteoalarm_area_description_map(areas_list)

    with progress_bar(transient=True) as progress:
        task = progress.add_task("Processing", total=len(new_alerts))
        for alert_id, alert_url in new_alerts:
            alert = await parser.parse_cap(alert_id, alert_url, areas_desc_map)
            if not alert or not alert.areas:  # pragma: no cover
                continue
            await store_alert(country, alert)
            counter += 1
            progress.update(task, advance=1)

    logger.info("Added %d new alerts", counter)

    for alert_id in parser.obsolete_alert_ids:
        await delete_alert(country, alert_id)

    logger.info("Removed %d obsolete alerts", len(parser.obsolete_alert_ids))

    alert_areas: dict[str, set[str]] = await get_alert_area_map(country)

    logger.info(
        "Total of %d alerts are available for %s",
        len(alert_areas),
        country.value,
    )

    # make area-alert mappings
    areas_with_alerts: set[str] = set()
    area_mappings: dict[str, set[str]] = {area.code: set() for area in areas_list}
    for alert_id, areas in alert_areas.items():
        for area in areas:
            area_mappings[area].add(alert_id)
            areas_with_alerts.add(area)

    for area, alerts in area_mappings.items():
        await store_alerts_for_area(country, area, alerts)

    logger.info("Areas with alerts: %d", len(areas_with_alerts))


async def get_alerts_and_notify(
    logger: Logger,
    country: CountryID,
    forecast: bool = False,
    dry_run: bool = False,
) -> None:
    """Get alerts and notify for a specific country."""
    from .notifications import (
        # send_forecast_notifications,
        send_start_notifications,
    )

    await get_alerts(logger, country)

    await send_start_notifications(logger, country, dry_run=dry_run)

    if forecast:
        # asyncio.run(send_forecast_notifications(logger, country))
        pass

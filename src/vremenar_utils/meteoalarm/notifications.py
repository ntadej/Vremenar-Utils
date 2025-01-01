"""MeteoAlarm notifications."""

from __future__ import annotations

from datetime import UTC, datetime

from babel.dates import format_datetime

from vremenar_utils.cli.common import CountryID, LanguageID
from vremenar_utils.cli.logging import Logger, progress_bar
from vremenar_utils.database.redis import redis
from vremenar_utils.notifications import BatchNotify, make_message, prepare_message

from .areas import load_meteoalarm_areas
from .common import AlertArea, AlertSeverity
from .database import BatchedNotifyOnset, get_alert_ids, get_alert_info

FORMAT = {
    LanguageID.English: "MMM d, h:mm a",
    LanguageID.German: "dd.MM., HH:mm",
    LanguageID.Slovenian: "d. MMM, HH:mm",
}
UNTIL = {
    LanguageID.English: "until",
    LanguageID.German: "bis",
    LanguageID.Slovenian: "do",
}


async def send_start_notifications(
    logger: Logger,
    country: CountryID,
    dry_run: bool = False,
) -> None:
    """Send notifications at the start of the alerts."""
    areas = {area.code: area for area in load_meteoalarm_areas(country)}

    existing_alerts: set[str] = await get_alert_ids(country)
    logger.info("Sending notifications")
    if dry_run:  # pragma: no branch
        logger.info("This is a dry run!")
    logger.info("Read %d existing alerts from the database", len(existing_alerts))

    async with redis.client() as db, BatchedNotifyOnset(db, country) as batch:
        with (
            BatchNotify(
                logger,
                dry_run=dry_run,
            ) as notifier,
            progress_bar(transient=True) as progress,
        ):
            task = progress.add_task("Processing", total=len(existing_alerts))
            for alert_id in existing_alerts:
                alert = await get_alert_info(country, alert_id)
                if int(alert["notifications"]["onset"]):
                    continue

                alert_onset = datetime.fromtimestamp(
                    float(alert["info"]["onset"][:-3]),
                    tz=UTC,
                )
                alert_expires = datetime.fromtimestamp(
                    float(alert["info"]["expires"][:-3]),
                    tz=UTC,
                )
                if alert_onset > datetime.now(tz=UTC):
                    continue
                if alert_expires < datetime.now(tz=UTC):
                    continue

                logger.info("Alert ID: %s", alert_id)

                send_start_notification(logger, notifier, alert, areas)

                if not dry_run:  # pragma: no cover
                    await batch.add(alert_id)

                progress.update(task, advance=1)


def send_start_notification(
    logger: Logger,
    notifier: BatchNotify,
    alert: dict[str, dict[str, str]],
    areas: dict[str, AlertArea],
) -> None:
    """Send notification at the start of an alert."""
    alert_severity = AlertSeverity(alert["info"]["severity"])
    alert_expires = datetime.fromtimestamp(
        float(alert["info"]["expires"][:-3]),
        tz=UTC,
    )

    for language in LanguageID:
        for area_code in alert["areas"]:
            until_string = format_datetime(
                alert_expires.astimezone(),
                FORMAT[language],
                locale=language.value,
            )
            message = make_message(
                alert[language.value]["event"],
                "",
                f"{areas[area_code].name},"
                f" {UNTIL[language]} {until_string}:"
                f" {alert[language.value]['description']}",
                important=True,
                expires=alert_expires,
                badge=1,
            )
            topics = [
                f"{language.value}_{severity}_{area_code}"
                for severity in alert_severity.topics()
            ]
            prepare_message(message, topics=topics, logger=logger)
            notifier.send(message)

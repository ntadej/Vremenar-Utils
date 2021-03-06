"""MeteoAlarm notifications."""
from babel.dates import format_datetime
from datetime import datetime

from ..cli.common import CountryID, LanguageID
from ..cli.logging import Logger
from ..database.redis import redis
from ..notifications import make_message, prepare_message, BatchNotify

from .areas import load_meteoalarm_areas
from .common import AlertSeverity
from .database import get_alert_ids, get_alert_info, BatchedNotifyOnset

FORMAT = {
    LanguageID.English: 'MMM d, h:mm a',
    LanguageID.German: 'dd.MM., HH:mm',
    LanguageID.Slovenian: 'd. MMM, HH:mm',
}
UNTIL = {
    LanguageID.English: 'until',
    LanguageID.German: 'bis',
    LanguageID.Slovenian: 'do',
}


async def send_start_notifications(logger: Logger, country: CountryID) -> None:
    """Send notifications at the start of the alerts."""
    # TODO: alerts per area
    areas = {area.code: area for area in load_meteoalarm_areas(country)}

    existing_alerts: set[str] = await get_alert_ids(country)
    logger.info(f'Read {len(existing_alerts)} existing alerts from the database')

    async with redis.client() as db:
        async with BatchedNotifyOnset(db) as batch:
            with BatchNotify(logger) as notifier:
                for id in existing_alerts:
                    alert = await get_alert_info(country, id)
                    if int(alert['notifications']['onset']):
                        continue

                    alert_severity = AlertSeverity(alert['info']['severity'])
                    alert_onset = datetime.fromtimestamp(
                        float(alert['info']['onset'][:-3])
                    )
                    alert_expires = datetime.fromtimestamp(
                        float(alert['info']['expires'][:-3])
                    )
                    if alert_onset > datetime.now():
                        continue
                    if alert_expires < datetime.now():
                        continue

                    logger.debug(f'Alert ID: {id}')

                    for language in LanguageID:
                        for area_code in alert['areas']:
                            until_string = format_datetime(
                                alert_expires,
                                FORMAT[language],
                                locale=language.value,
                            )
                            message = make_message(
                                alert[language.value]['event'],
                                '',
                                f'{areas[area_code].name},'
                                f' {UNTIL[language]} {until_string}:'
                                f" {alert[language.value]['description']}",
                                important=True,
                                expires=alert_expires,
                                badge=1,
                            )
                            topics = []
                            for severity in alert_severity.topics():
                                topics.append(
                                    f'{language.value}_{severity}_{area_code}'
                                )
                            prepare_message(logger, message, topics=topics)
                            notifier.send(message)

                    await batch.add({'country': country, 'id': id})


async def send_forecast_notifications(logger: Logger, country: CountryID) -> None:
    """Send notifications with daily alert forecasts."""
    pass

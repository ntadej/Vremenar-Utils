"""Vremenar Utils CLI."""
import asyncio
import sys
import typer
from typing import Optional

from .common import CountryID
from .logging import colors, setup_logger, style

from .. import __version__
from ..database.redis import database_info

if not sys.warnoptions:
    import warnings

    warnings.simplefilter('default')

application = typer.Typer()


def version_callback(value: bool) -> None:
    """Version callback."""
    if value:
        typer.echo(f'Vremenar Utils, version {__version__}')
        raise typer.Exit()


@application.callback()
def main(
    version: Optional[bool] = typer.Option(  # noqa: B008
        None,
        '--version',
        help='Show version and exit.',
        callback=version_callback,
        is_eager=True,
    ),
) -> None:
    """Vremenar Utilities CLI app."""
    pass


@application.command()
def stations_store(
    country: CountryID = typer.Argument(..., help='Country'),  # noqa: B008
) -> None:
    """Load stations into the database."""
    logger = setup_logger()

    base_message = 'Storing stations into database for country'
    message = f'{base_message} %s'
    color_message = f'{base_message} {style("%s", fg=colors.CYAN)}'
    logger.info(message, country.label(), extra={'color_message': color_message})

    database_info(logger)

    if country is CountryID.Germany:
        from ..dwd.database import store_stations as dwd_store_stations

        asyncio.run(dwd_store_stations(logger))
    elif country is CountryID.Slovenia:
        from ..arso.database import store_stations as arso_store_stations

        asyncio.run(arso_store_stations(logger))


@application.command()
def dwd_mosmix(
    job: Optional[int] = typer.Argument(  # noqa: B008
        None, help='Job number for batched processing'
    ),
    local_source: Optional[bool] = typer.Option(  # noqa: B008
        False, '--local-source', help="Use local 'MOSMIX_S_LATEST_240.kmz'."
    ),
    local_stations: Optional[bool] = typer.Option(  # noqa: B008
        False, '--local-stations', help='Use local stations database.'
    ),
) -> None:
    """DWD weather MOSMIX data caching."""
    logger = setup_logger('dwd_mosmix')

    message = 'Processing MOSMIX data for Germany'
    color_message = f'Processing {style("MOSMIX", fg=colors.CYAN)} data for Germany'
    logger.info(message, extra={'color_message': color_message})

    database_info(logger)

    from ..dwd.forecast import process_mosmix

    asyncio.run(
        process_mosmix(
            logger,
            local_source=local_source,
            local_stations=local_stations,
        )
    )


@application.command()
def dwd_current() -> None:
    """DWD current weather data caching."""
    logger = setup_logger('dwd_current')

    message = 'Processing current weather data for Germany'
    color_message = (
        f'Processing {style("current weather", fg=colors.CYAN)} data for Germany'
    )
    logger.info(message, extra={'color_message': color_message})

    database_info(logger)

    from ..dwd.current import current_weather

    asyncio.run(current_weather(logger))


@application.command()
def dwd_stations(
    output: Optional[str] = typer.Argument(  # noqa: B008
        default='DWD.csv', help='Output file'
    ),
    output_new: Optional[str] = typer.Argument(  # noqa: B008
        default='DWD.NEW.csv', help='Output file for new stations'
    ),
    local_source: Optional[bool] = typer.Option(  # noqa: B008
        False, '--local-source', help="Use local 'MOSMIX_S_LATEST_240.kmz'."
    ),
) -> None:
    """DWD process stations."""
    logger = setup_logger()

    from ..dwd.stations import process_mosmix_stations

    asyncio.run(
        process_mosmix_stations(
            logger,
            output if output else 'DWD.csv',
            output_new if output_new else 'DWD.NEW.csv',
            local_source=local_source,
        )
    )


@application.command()
def alerts_areas(
    country: CountryID = typer.Argument(..., help='Country'),  # noqa: B008
    output: Optional[str] = typer.Argument(  # noqa: B008
        default='areas.json', help='Output file'
    ),
    output_matches: Optional[str] = typer.Argument(  # noqa: B008
        default='matches.json', help='Output file for area-station matches'
    ),
) -> None:
    """Load MeteoAlarm areas."""
    logger = setup_logger()

    base_message = 'Processing weather alerts areas for country'
    message = f'{base_message} %s'
    color_message = f'{base_message} {style("%s", fg=colors.CYAN)}'
    logger.info(message, country.label(), extra={'color_message': color_message})

    database_info(logger)

    from ..meteoalarm.areas import process_meteoalarm_areas

    asyncio.run(
        process_meteoalarm_areas(
            logger,
            country,
            output if output else 'areas.json',
            output_matches if output_matches else 'matches.json',
        )
    )


@application.command()
def alerts_get(
    country: CountryID = typer.Argument(..., help='Country'),  # noqa: B008
) -> None:
    """Load MeteoAlarm alerts."""
    logger = setup_logger('meteoalarm')

    base_message = 'Processing weather alerts for country'
    message = f'{base_message} %s'
    color_message = f'{base_message} {style("%s", fg=colors.CYAN)}'
    logger.info(message, country.label(), extra={'color_message': color_message})

    database_info(logger)

    from ..meteoalarm.steering import get_alerts

    asyncio.run(get_alerts(logger, country))


@application.command()
def alerts_notify(
    country: CountryID = typer.Argument(..., help='Country'),  # noqa: B008
    forecast: Optional[bool] = typer.Option(  # noqa: B008
        False, '--forecast', help='Send forecast notification.'
    ),
) -> None:
    """Notify MeteoAlarm alerts."""
    logger = setup_logger('meteoalarm_notify')

    base_message = 'Processing weather alerts notifications for country'
    message = f'{base_message} %s'
    color_message = f'{base_message} {style("%s", fg=colors.CYAN)}'
    logger.info(message, country.label(), extra={'color_message': color_message})

    database_info(logger)

    from ..meteoalarm.notifications import (
        send_start_notifications,
        send_forecast_notifications,
    )

    if forecast:
        asyncio.run(send_forecast_notifications(logger, country))
    else:
        asyncio.run(send_start_notifications(logger, country))


__all__ = ['application']

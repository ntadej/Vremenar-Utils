"""Vremenar Utils CLI."""
import asyncio
import sys
import typer
from typing import Optional

from .common import CountryID
from .logging import colors, setup_logger, style

from .. import __version__
from ..arso.database import store_stations as arso_store_stations
from ..database.redis import database_info
from ..dwd.database import store_stations as dwd_store_stations
from ..dwd.forecast import (
    process_mosmix as dwd_process_mosmix,
    cleanup_mosmix as dwd_cleanup_mosmix,
)
from ..dwd.stations import process_mosmix_stations as dws_mosmix_stations
from ..meteoalarm.areas import process_meteoalarm_areas as meteoalarm_areas
from ..meteoalarm.steering import get_alerts as meteoalarm_alerts_get

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
    return


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
        asyncio.run(dwd_store_stations(logger))
    elif country is CountryID.Slovenia:
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

    dwd_process_mosmix(
        logger,
        job=job,
        disable_cache=False,
        local_source=local_source,
        local_stations=local_stations,
    )


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

    dws_mosmix_stations(
        logger,
        output if output else 'DWD.csv',
        output_new if output_new else 'DWD.NEW.csv',
        local_source=local_source,
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

    asyncio.run(
        meteoalarm_areas(
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

    asyncio.run(meteoalarm_alerts_get(logger, country))


@application.command()
def cleanup() -> None:
    """Cleanup obsolete local caches."""
    dwd_cleanup_mosmix()


__all__ = ['application']

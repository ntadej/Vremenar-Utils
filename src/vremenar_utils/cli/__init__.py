"""Vremenar Utils CLI."""
import typer
from typing import Optional

from .. import __version__
from ..dwd.forecast import (
    process_mosmix as dwd_process_mosmix,
    cleanup_mosmix as dwd_cleanup_mosmix,
)
from ..dwd.stations import process_mosmix_stations as dws_mosmix_stations
from ..meteoalarm.areas import process_meteoalarm_areas as meteoalarm_areas
from ..meteoalarm.common import AlarmCountry

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
def dwd_mosmix(
    job: Optional[int] = typer.Argument(  # noqa: B008
        None, help='Job number for batched processing'
    ),
    use_database: Optional[bool] = typer.Option(  # noqa: B008
        False, '--database', help='Update database.'
    ),
    local_source: Optional[bool] = typer.Option(  # noqa: B008
        False, '--local-source', help="Use local 'MOSMIX_S_LATEST_240.kmz'."
    ),
    local_stations: Optional[bool] = typer.Option(  # noqa: B008
        False, '--local-stations', help='Use local stations database.'
    ),
) -> None:
    """DWD weather MOSMIX data caching."""
    dwd_process_mosmix(
        job=job,
        disable_database=not use_database,
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
    use_database: Optional[bool] = typer.Option(  # noqa: B008
        False, '--database', help='Update database.'
    ),
    local_source: Optional[bool] = typer.Option(  # noqa: B008
        False, '--local-source', help="Use local 'MOSMIX_S_LATEST_240.kmz'."
    ),
) -> None:
    """DWD process stations."""
    dws_mosmix_stations(
        output if output else 'DWD.csv',
        output_new if output_new else 'DWD.NEW.csv',
        disable_database=not use_database,
        local_source=local_source,
    )


@application.command()
def warnings_areas(
    country: AlarmCountry = typer.Argument(..., help='Country'),  # noqa: B008
    output: Optional[str] = typer.Argument(  # noqa: B008
        default='areas.json', help='Output file'
    ),
    output_matches: Optional[str] = typer.Argument(  # noqa: B008
        default='matches.json', help='Output file for area-station matches'
    ),
) -> None:
    """Load MeteoAlarm areas."""
    meteoalarm_areas(
        country,
        output if output else 'areas.json',
        output_matches if output_matches else 'matches.json',
    )


@application.command()
def cleanup() -> None:
    """Cleanup obsolete local caches."""
    dwd_cleanup_mosmix()


__all__ = ['application']

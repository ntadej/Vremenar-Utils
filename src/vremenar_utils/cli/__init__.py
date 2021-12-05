"""Vremenar Utils CLI."""
import typer
from typing import Optional

from .. import __version__
from ..dwd.current import current_weather as dwd_current_weather
from ..dwd.forecast import process_mosmix as dwd_mosmix
from ..dwd.stations import process_mosmix_stations as dws_mosmix_stations

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
def dwd(
    job: Optional[int] = typer.Argument(  # noqa: B008
        None, help='Job number for batched processing'
    ),
    current: Optional[bool] = typer.Option(  # noqa: B008
        False, '--current', help='Update current weather information.'
    ),
    use_database: Optional[bool] = typer.Option(  # noqa: B008
        False, '--database', help='Update database.'
    ),
) -> None:
    """DWD weather data caching."""
    if current:
        dwd_current_weather(disable_database=not use_database)
    else:
        dwd_mosmix(disable_database=not use_database, disable_cache=False, job=job)


@application.command()
def dwd_stations(
    output: Optional[str] = typer.Argument(  # noqa: B008
        default='DWD.csv', help='Output file'
    ),
    use_database: Optional[bool] = typer.Option(  # noqa: B008
        False, '--database', help='Update database.'
    ),
) -> None:
    """DWD process stations."""
    dws_mosmix_stations(
        output if output else 'DWD.csv', disable_database=not use_database
    )


__all__ = ['application']

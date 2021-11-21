"""Vremenar Utils CLI."""
import typer
from typing import Optional

from .. import __version__
from ..dwd.current import current_weather as dwd_current_weather

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
    current: Optional[bool] = typer.Option(  # noqa: B008
        False, '--current', help='Update current weather information.'
    )
) -> None:
    """DWD weather data utilities."""
    if current:
        dwd_current_weather()


__all__ = ['application']

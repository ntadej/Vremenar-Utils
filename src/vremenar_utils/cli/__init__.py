"""Vremenar Utils CLI."""
import asyncio
import sys
from pathlib import Path
from sys import argv
from typing import Annotated, Optional

import typer

from vremenar_utils import __version__
from vremenar_utils.database.redis import init_database

from .common import CountryID, DatabaseType
from .config import (
    TyperState,
    config_missing,
    generate_empty_config,
    init_config,
    print_config_file,
)
from .logging import colors, setup_logger, style

if not sys.warnoptions:  # pragma: no cover
    import warnings

    warnings.simplefilter("default")

DatabaseAnnotatedType = Optional[DatabaseType]  # noqa: UP007

application = typer.Typer()
state = TyperState()


def version_callback(value: bool) -> None:
    """Version callback."""
    if value:
        typer.echo(f"Vremenar Utils, version {__version__}")
        raise typer.Exit()


@application.callback()
def main(
    ctx: typer.Context,
    config: Annotated[
        Path,
        typer.Option(
            "-c",
            "--config",
            envvar="VREMENAR_UTILS_CONFIG",
            help="Configuration file.",
        ),
    ] = Path(
        "config.yml",
    ),
    database: Annotated[
        DatabaseAnnotatedType,
        typer.Option(
            "--database",
            envvar="VREMENAR_DATABASE",
            help="Choose which database to use.",
        ),
    ] = None,
    debug: Annotated[
        bool,
        typer.Option(
            "--debug",
            help="Run with debug printouts.",
        ),
    ] = False,
    version: Annotated[  # noqa: ARG001
        bool,
        typer.Option(
            "--version",
            help="Show version and exit.",
            callback=version_callback,
            is_eager=True,
        ),
    ] = False,
) -> None:
    """Vremenar Utilities CLI app."""
    if ctx.invoked_subcommand != "config" and not config.exists():
        if "--help" in argv:
            return
        config_missing(config)

    state.config_file = config
    state.debug = debug
    state.database_type = database


@application.command()
def config(
    generate: Annotated[
        bool,
        typer.Option("--generate", help="Generate empty configuration."),
    ] = False,
) -> None:
    """Print or generate configuration."""
    if generate:
        generate_empty_config(state.config_file)
    else:
        print_config_file(state.config_file)
        init_config(state)


@application.command()
def update_crontab() -> None:
    """Update crontab."""
    config = init_config(state)
    logger = setup_logger(config)

    from vremenar_utils.cli.crontab import setup_crontab

    setup_crontab(logger, config)


@application.command()
def stations_store(
    country: Annotated[CountryID, typer.Argument(..., help="Country")],
) -> None:
    """Load stations into the database."""
    config = init_config(state)
    logger = setup_logger(config)

    base_message = "Storing stations into database for country"
    message = f"{base_message} %s"
    color_message = f'{base_message} {style("%s", fg=colors.CYAN)}'
    logger.info(message, country.label(), extra={"color_message": color_message})

    init_database(logger, config)

    if country is CountryID.Germany:
        from vremenar_utils.dwd.database import store_stations as dwd_store_stations

        asyncio.run(dwd_store_stations(logger))
    elif country is CountryID.Slovenia:
        from vremenar_utils.arso.database import store_stations as arso_store_stations

        asyncio.run(arso_store_stations(logger))
    else:  # pragma: no cover
        raise RuntimeError()


@application.command()
def arso_weather(
    local_stations: Annotated[
        bool,
        typer.Option("--local-stations", help="Use local stations database."),
    ] = False,
) -> None:
    """ARSO weather conditions data caching."""
    config = init_config(state)
    logger = setup_logger(config, "arso_weather")

    message = "Processing ARSO weather conditions data for Slovenia"
    color_message = (
        f'Processing {style("ARSO weather conditions", fg=colors.CYAN)}'
        'data for Slovenia'
    )
    logger.info(message, extra={"color_message": color_message})

    init_database(logger, config)

    from vremenar_utils.arso.weather import process_weather_data

    asyncio.run(
        process_weather_data(
            logger,
            local_stations=local_stations,
        ),
    )


@application.command()
def dwd_mosmix(
    local_source: Annotated[
        bool,
        typer.Option("--local-source", help="Use local 'MOSMIX_S_LATEST_240.kmz'."),
    ] = False,
    local_stations: Annotated[
        bool,
        typer.Option("--local-stations", help="Use local stations database."),
    ] = False,
) -> None:
    """DWD weather MOSMIX data caching."""
    config = init_config(state)
    logger = setup_logger(config, "dwd_mosmix")

    message = "Processing MOSMIX data for Germany"
    color_message = f'Processing {style("MOSMIX", fg=colors.CYAN)} data for Germany'
    logger.info(message, extra={"color_message": color_message})

    init_database(logger, config)

    from vremenar_utils.dwd.forecast import process_mosmix

    asyncio.run(
        process_mosmix(
            logger,
            local_source=local_source,
            local_stations=local_stations,
        ),
    )


@application.command()
def dwd_current(
    test_mode: Annotated[
        bool,
        typer.Option("--test-mode", help="Only run as a test on a few stations."),
    ] = False,
) -> None:
    """DWD current weather data caching."""
    config = init_config(state)
    logger = setup_logger(config, "dwd_current")

    message = "Processing current weather data for Germany"
    color_message = (
        f'Processing {style("current weather", fg=colors.CYAN)} data for Germany'
    )
    logger.info(message, extra={"color_message": color_message})

    init_database(logger, config)

    from vremenar_utils.dwd.current import current_weather

    asyncio.run(current_weather(logger, test_mode if test_mode else False))


@application.command()
def dwd_stations(
    output: Annotated[Path, typer.Argument(help="Output file")] = Path("DWD.csv"),
    output_new: Annotated[
        Path,
        typer.Argument(help="Output file for new stations"),
    ] = Path("DWD.NEW.csv"),
    local_source: Annotated[
        bool,
        typer.Option("--local-source", help="Use local 'MOSMIX_S_LATEST_240.kmz'."),
    ] = False,
) -> None:
    """DWD process stations."""
    config = init_config(state)
    logger = setup_logger(config)

    from vremenar_utils.dwd.stations import process_mosmix_stations

    asyncio.run(
        process_mosmix_stations(logger, output, output_new, local_source=local_source),
    )


@application.command()
def alerts_areas(
    country: Annotated[CountryID, typer.Argument(..., help="Country")],
    output: Annotated[Path, typer.Argument(help="Output file")] = Path("areas.json"),
    output_matches: Annotated[
        Path,
        typer.Argument(help="Output file for area-station matches"),
    ] = Path("matches.json"),
) -> None:
    """Load MeteoAlarm areas."""
    config = init_config(state)
    logger = setup_logger(config)

    base_message = "Processing weather alerts areas for country"
    message = f"{base_message} %s"
    color_message = f'{base_message} {style("%s", fg=colors.CYAN)}'
    logger.info(message, country.label(), extra={"color_message": color_message})

    init_database(logger, config)

    from vremenar_utils.meteoalarm.areas import process_meteoalarm_areas

    asyncio.run(
        process_meteoalarm_areas(logger, country, output, output_matches),
    )


@application.command()
def alerts_get(
    country: Annotated[CountryID, typer.Argument(..., help="Country")],
) -> None:
    """Load MeteoAlarm alerts."""
    config = init_config(state)
    logger = setup_logger(config, "meteoalarm")

    base_message = "Processing weather alerts for country"
    message = f"{base_message} %s"
    color_message = f'{base_message} {style("%s", fg=colors.CYAN)}'
    logger.info(message, country.label(), extra={"color_message": color_message})

    init_database(logger, config)

    from vremenar_utils.meteoalarm.steering import get_alerts

    asyncio.run(get_alerts(logger, country))


@application.command()
def alerts_notify(
    country: Annotated[CountryID, typer.Argument(..., help="Country")],
    forecast: Annotated[
        bool,
        typer.Option("--forecast", help="Send forecast notification."),
    ] = False,
) -> None:
    """Notify MeteoAlarm alerts."""
    config = init_config(state)
    logger = setup_logger(config, "meteoalarm_notify")

    base_message = "Processing weather alerts notifications for country"
    message = f"{base_message} %s"
    color_message = f'{base_message} {style("%s", fg=colors.CYAN)}'
    logger.info(message, country.label(), extra={"color_message": color_message})

    init_database(logger, config)

    from vremenar_utils.meteoalarm.notifications import (
        # send_forecast_notifications,
        send_start_notifications,
    )

    if forecast:
        # asyncio.run(send_forecast_notifications(logger, country))
        pass
    else:
        asyncio.run(send_start_notifications(logger, country))


@application.command()
def alerts_update(
    country: Annotated[CountryID, typer.Argument(..., help="Country")],
    forecast: Annotated[
        bool,
        typer.Option("--forecast", help="Send forecast notification."),
    ] = False,
) -> None:
    """Load and notify MeteoAlarm alerts."""
    config = init_config(state)
    logger = setup_logger(config, "meteoalarm")

    base_message = "Processing weather alerts and notifying for country"
    message = f"{base_message} %s"
    color_message = f'{base_message} {style("%s", fg=colors.CYAN)}'
    logger.info(message, country.label(), extra={"color_message": color_message})

    init_database(logger, config)

    from vremenar_utils.meteoalarm.steering import get_alerts_and_notify

    asyncio.run(get_alerts_and_notify(logger, country, forecast))


__all__ = ["application"]

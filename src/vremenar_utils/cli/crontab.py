"""Crontab utilities."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from crontab import CronItem, CronTab
from rich import print as rprint
from rich.prompt import Confirm

from .common import CountryID, DatabaseType

if TYPE_CHECKING:
    from .config import Configuration
    from .logging import Logger

COMMAND_LIST: list[str] = [
    "alerts-update",
    "arso-maps",
    "arso-weather",
    "dwd-current",
    "dwd-mosmix",
]
COMMAND_LIST_PER_COUNTRY: list[str] = ["alerts-update"]


def set_cron_item_interval(cron: CronItem, command: str, db_type: DatabaseType) -> None:
    """Set cron item interval for a specific command."""
    if command in ["alerts-update", "arso-maps"]:
        cron.minute.every(  # type: ignore[union-attr]
            2 if db_type == DatabaseType.Production else 5,
        )
    elif command in ["arso-weather", "dwd-current"]:
        if db_type == DatabaseType.Production:
            cron.minute.every(15)  # type: ignore[union-attr]
        else:
            cron.minute.on(45)  # type: ignore[union-attr]
    elif command == "dwd-mosmix":
        if db_type == DatabaseType.Production:
            cron.minute.on(35)  # type: ignore[union-attr]
        else:
            cron.minute.on(40)  # type: ignore[union-attr]
    else:
        error = f"Unknown command: {command}"
        raise ValueError(error)


def setup_command(  # noqa: PLR0913
    config: Configuration,
    utils_path: Path,
    db_type: DatabaseType,
    command: str,
    uuid: str,
    arguments: str = "",
) -> str:
    """Prepare crontab command."""
    runitor_command = ""
    if config.runitor_enabled:
        runitor_command = f"runitor -api-url {config.runitor_ping_url} -uuid {uuid} --"

    pdm_command = f"pdm run -p {utils_path} vremenar_utils"
    command_config = (
        f"--config {config.path} --database {db_type.value} {command} {arguments}"
    )

    command_final = f"{runitor_command} nice {pdm_command} {command_config}"
    command_final = command_final.strip()
    command_final += " >/dev/null 2>&1"
    return command_final.strip()


def setup_crontab(logger: Logger, config: Configuration) -> None:  # noqa: C901, PLR0912, PLR0915
    """Prepare crontab for Vremenar Utils."""
    cron = CronTab(user=True)

    utils_path = Path(__file__).resolve().parent.parent.parent.parent
    logger.info("Vremenar Utils path: %s", utils_path)

    # remove existing
    if cron.crons:
        to_remove = [
            job
            for job in cron.crons
            for command in COMMAND_LIST
            if job.command and command in job.command
        ]
        for job in to_remove:
            cron.remove(job)

    # add missing jobs
    for db_type_entry, commands_dict in config.commands.items():
        try:
            db_type = DatabaseType(db_type_entry)
        except ValueError:
            logger.warning("Unknown database type: %s", db_type_entry)
            continue

        if not commands_dict:
            continue

        first = True

        for command, uuid in commands_dict.items():
            if command not in COMMAND_LIST:
                logger.warning("Unknown command: %s", command)
                continue

            if command in COMMAND_LIST_PER_COUNTRY:
                if not isinstance(uuid, dict):
                    continue

                for country in CountryID:
                    if country.value not in uuid:
                        continue

                    command_string = setup_command(
                        config,
                        utils_path,
                        db_type,
                        command,
                        uuid[country.value],
                        country.value,
                    )

                    if first:
                        job = cron.new(
                            command=command_string,
                            comment=f"Vremenar Utils: {db_type.value}",
                            pre_comment=True,
                        )
                        first = False
                    else:
                        job = cron.new(command=command_string)
                    set_cron_item_interval(job, command, db_type)

            else:
                if not isinstance(uuid, str):
                    continue

                command_string = setup_command(
                    config,
                    utils_path,
                    db_type,
                    command,
                    uuid,
                )

                if first:
                    job = cron.new(
                        command=command_string,
                        comment=f"Vremenar Utils: {db_type.value}",
                        pre_comment=True,
                    )
                    first = False
                else:
                    job = cron.new(command=command_string)
                set_cron_item_interval(job, command, db_type)

    if not cron.crons or not cron.lines:
        error = "No crontab jobs found!"
        raise RuntimeError(error)

    # print new status
    logger.info("Crontab to write:")
    for line in cron.lines:
        logger.info("%s", line)

    # ask to write changes
    rprint()
    confirmation = Confirm.ask("Do you want to save this crontab?", default=False)

    if confirmation:
        rprint()
        logger.info("Saving crontab...")
        cron.write()

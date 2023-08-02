"""Crontab utilities."""
from pathlib import Path

from crontab import CronItem, CronTab
from rich import print
from rich.prompt import Confirm

from vremenar_utils.database.redis import DatabaseType

from .config import Configuration
from .logging import Logger

COMMAND_LIST: list[str] = ["alerts-update", "arso-weather", "dwd-current", "dwd-mosmix"]


def set_cron_item_interval(cron: CronItem, command: str, db_type: DatabaseType) -> None:
    """Set cron item interval for a specific command."""
    if command in ["alerts-update"]:
        cron.minute.every(  # type: ignore
            2 if db_type == DatabaseType.Production else 5,
        )
    elif command in ["arso-weather", "dwd-current"]:
        if db_type == DatabaseType.Production:
            cron.minute.every(15)  # type: ignore
        else:
            cron.minute.on(45)  # type: ignore
    elif command == "dwd-mosmix":
        if db_type == DatabaseType.Production:
            cron.minute.on(35)  # type: ignore
        else:
            cron.minute.on(40)  # type: ignore
    else:
        error = f"Unknown command: {command}"
        raise ValueError(error)


def setup_crontab(logger: Logger, config: Configuration) -> None:  # noqa: C901, PLR0912
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
        except ValueError:  # noqa: PERF203
            logger.warning("Unknown database type: %s", db_type_entry)
            continue

        if not commands_dict:
            continue

        first = True

        for command, uuid in commands_dict.items():
            if command not in COMMAND_LIST:
                logger.warning("Unknown command: %s", command)
                continue

            runitor_command = ""
            if config.runitor_enabled:
                if not uuid:
                    logger.warning(
                        "Command %s does not have UUID set but runitor enabled",
                        command,
                    )
                    continue

                runitor_command = (
                    f"runitor -api-url {config.runitor_ping_url} -uuid {uuid} --"
                )

            poetry_command = f"poetry -C {utils_path} run vremenar_utils"
            command_config = (
                f"--config {config.path} --database {db_type.value} {command}"
            )

            command_final = (
                f"{runitor_command} nice {poetry_command} {command_config}".strip()
            )

            if first:
                job = cron.new(
                    command=command_final,
                    comment=f"Vremenar Utils: {db_type.value}",
                    pre_comment=True,
                )
                first = False
            else:
                job = cron.new(command=command_final)
            set_cron_item_interval(job, command, db_type)

    assert cron.crons
    assert cron.lines

    # print new status
    logger.info("Crontab to write:")
    for line in cron.lines:
        logger.info("%s", line)

    # ask to write changes
    print()
    confirmation = Confirm.ask("Do you want to save this crontab?", default=False)

    if confirmation:
        print()
        logger.info("Saving crontab...")
        cron.write()

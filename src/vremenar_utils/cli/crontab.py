"""Crontab utilities."""
from pathlib import Path

from crontab import CronItem, CronTab
from rich import print
from rich.prompt import Confirm

from vremenar_utils.database.redis import DatabaseType

from .config import Configuration
from .logging import Logger


def set_cron_item_interval(cron: CronItem, command: str) -> None:
    """Set cron item interval for a specific command."""
    if command in ["arso-weather", "dwd-current"]:
        cron.minute.every(15)  # type: ignore
    elif command == "dwd-mosmix":
        cron.minute.on(35)  # type: ignore
    else:
        error = f"Unknown command: {command}"
        raise ValueError(error)


def setup_crontab(logger: Logger, config: Configuration) -> None:  # noqa: C901, PLR0912
    """Prepare crontab for Vremenar Utils."""
    cron = CronTab(user=True)

    utils_path = Path(__file__).resolve().parent.parent.parent.parent
    logger.info("Vremenar Utils path: %s", utils_path)

    # remove existing
    commands_list = ["arso-weather", "dwd-current", "dwd-mosmix"]
    if cron.crons:
        for job in cron.crons:
            for command in commands_list:
                if job.command and command in job.command:
                    cron.remove(job)

    # add missing jobs
    for db_type_entry, commands_dict in config.commands.items():
        try:
            db_type = DatabaseType(db_type_entry)
        except ValueError:  # noqa: PERF203
            logger.warning("Unknown database type: %s", db_type_entry)
            continue

        for command, uuid in commands_dict.items():
            if command not in commands_list:
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

            job = cron.new(command=command_final)
            set_cron_item_interval(job, command)

    assert cron.crons

    # print new status
    for job in cron.crons:
        logger.info("Crontab job: %s", job)

    # ask to write changes
    print()
    confirmation = Confirm.ask("Do you want to save this crontab?", default=False)

    if confirmation:
        print()
        logger.info("Saving crontab...")

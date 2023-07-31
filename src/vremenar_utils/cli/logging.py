"""Common logging setup."""
from __future__ import annotations

import logging
from logging import Logger
from typing import TYPE_CHECKING, Any

from click import style
from rich import print
from rich.color import Color
from rich.logging import RichHandler
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)
from rich.style import Style
from typer import Exit, colors

if TYPE_CHECKING:
    from .config import Configuration


def info_panel(message: str, title: str = "Information") -> None:
    """Print info message in a panel."""
    print(
        Panel(
            message,
            title=title,
            title_align="left",
            border_style=Style(color=Color.parse("blue")),
        ),
    )


def error_panel(message: str) -> Exit:
    """Print error message in a panel."""
    print(
        Panel(
            message,
            title="Error",
            title_align="left",
            border_style=Style(color=Color.parse("red")),
        ),
    )
    return Exit(1)


def progress_bar(**kwargs: Any) -> Progress:  # noqa: ANN401
    """Return progress bar."""
    return Progress(
        TextColumn("[progress.description]{task.description:>27} "),
        BarColumn(bar_width=None),
        TaskProgressColumn(),
        TimeRemainingColumn(),
        **kwargs,
    )


def download_bar(**kwargs: Any) -> Progress:  # noqa: ANN401
    """Return download bar."""
    return Progress(
        TextColumn("[progress.description]{task.description:>27} "),
        BarColumn(bar_width=None),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
        **kwargs,
    )


def setup_logger(config: Configuration, name: str | None = None) -> logging.Logger:
    """Prepare logger and write the log file."""
    if name:
        file_formatter = logging.Formatter(
            "%(asctime)s %(levelname)-8s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_path = config.log_path / f"{name}.log"
        file_handler = logging.FileHandler(file_path, mode="a")
        file_handler.setFormatter(file_formatter)

    stream_handler = RichHandler(
        show_path=config.debug,
        log_time_format="%Y-%m-%d %H:%M:%S",
    )

    logger = logging.getLogger()
    if name:
        logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    if config.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
        # Disable logging from other modules
        logging.getLogger("httpx").setLevel(logging.WARNING)

    return logger


__all__ = ["colors", "style", "Logger"]

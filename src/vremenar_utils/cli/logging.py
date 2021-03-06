"""Common logging setup."""
import logging
from logging import Logger
from click import style
from copy import copy
from sys import stdout
from typer import colors
from typing import Any, Callable, Literal, Optional, Union


class ColourizedFormatter(logging.Formatter):
    """Custom colourized formatter.

    A custom log formatter class that:
    * Outputs the LOG_LEVEL with an appropriate color.
    * If a log call includes an `extras={"color_message": ...}` it will be used
      for formatting the output, instead of the plain text message.
    """

    level_name_colors: dict[int, Callable[[Any], str]] = {
        logging.DEBUG: lambda level_name: style(str(level_name), colors.CYAN),
        logging.INFO: lambda level_name: style(str(level_name), colors.GREEN),
        logging.WARNING: lambda level_name: style(str(level_name), colors.YELLOW),
        logging.ERROR: lambda level_name: style(str(level_name), colors.RED),
        logging.CRITICAL: lambda level_name: style(
            str(level_name), fg=colors.BRIGHT_RED
        ),
    }

    def __init__(
        self,
        fmt: Optional[str] = None,
        datefmt: Optional[str] = '%Y-%m-%d %H:%M:%S',
        style: Union[Literal['%'], Literal['{'], Literal['$']] = '%',
    ):
        """Initialize logger."""
        self.use_colors = stdout.isatty()
        super().__init__(fmt=fmt, datefmt=datefmt, style=style)

    def _color_level_name(self, level_name: str, level_no: int) -> str:
        def default(level_name: str) -> str:
            return str(level_name)  # pragma: no cover

        func = self.level_name_colors.get(level_no, default)
        return func(level_name)

    def formatMessage(self, record: logging.LogRecord) -> str:  # noqa: N802
        """Format the message."""
        recordcopy = copy(record)
        levelname = recordcopy.levelname
        seperator = ' ' * (8 - len(recordcopy.levelname))
        if self.use_colors:
            levelname = self._color_level_name(levelname, recordcopy.levelno)
            if 'color_message' in recordcopy.__dict__:
                recordcopy.msg = recordcopy.__dict__['color_message']
                recordcopy.__dict__['message'] = recordcopy.getMessage()
        recordcopy.__dict__['levelprefix'] = levelname + ':' + seperator
        return super().formatMessage(recordcopy)


def setup_logger(name: Optional[str] = None) -> logging.Logger:
    """Prepare logger and write the log file."""
    if name:
        file_formatter = logging.Formatter(
            '%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler = logging.FileHandler(f'./{name}.log', mode='a')
        file_handler.setFormatter(file_formatter)

    stream_formatter = ColourizedFormatter('%(levelprefix)s %(message)s')
    stream_handler = logging.StreamHandler(stream=stdout)
    stream_handler.setFormatter(stream_formatter)

    logger = logging.getLogger()
    if name:
        logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.setLevel(logging.DEBUG)

    return logger


__all__ = ['setup_logger', 'colors', 'style', 'Logger']

"""Configuration utilities."""
from __future__ import annotations

from os import environ
from pathlib import Path
from typing import Any

import yaml

from .common import DatabaseType
from .logging import error_panel, info_panel


class TyperState:
    """Execution configuration state."""

    def __init__(self) -> None:
        """Initialize configuration state."""
        self.config_file: Path = Path("config.yml")
        self.debug: bool = False
        self.database_type: DatabaseType | None = None


class Configuration:
    """Configuration helper."""

    def __init__(self, path: Path) -> None:
        """Initialize configuration helper."""
        self.mode: str = "staging"
        self.debug: bool = False
        self.path: Path = path
        self.log_disabled: bool = False
        self.log_path: Path = Path()
        self.database_type: DatabaseType = DatabaseType.Staging
        self.firebase_credentials: Path = Path()

        self.commands: dict[str, dict[str, str | dict[str, str]]] = {}
        self.runitor_enabled: bool = False
        self.runitor_ping_url: str = ""

    def to_object(self) -> dict[str, Any]:
        """Convert configuration to object."""
        obj = {
            "Mode": self.mode,
            "Database": self.database_type.value,
            "Logging": {
                "disabled": str(self.log_disabled),
                "path": str(self.log_path),
                "debug": self.debug,
            },
        }
        if (
            not self.firebase_credentials.is_dir()
            and self.firebase_credentials.exists()
        ):
            obj["Firebase credentials"] = str(self.firebase_credentials)
        return obj


def config_missing(config_file: Path) -> None:
    """Print config missing message."""
    error_message = (
        f"Configuration file [blue]'{config_file}'[/blue] does not exist.\n"
        "Please run"
        " [blue]'vremenar config [bold]--generate[/bold]'[/blue]"
        " to generate it.\n"
        "Optionally you can specify the path using the"
        " [blue]'[bold]--config[/bold]'[/blue] option"
        " or using the environment variable"
        " [blue bold]VREMENAR_UTILS_CONFIG[/blue bold].]"
    )
    raise error_panel(error_message)


def generate_empty_config(config_file: Path) -> None:
    """Generate empty config file."""
    if config_file.exists():
        error_message = (
            f"Configuration file [blue]'{config_file}'[/blue] already exists."
        )
        raise error_panel(error_message)

    config = {
        "default_mode": "staging",
        "logging": {
            "path": "",
            "disabled": True,
        },
        "firebase": {
            "staging": "",
            "production": "",
        },
        "commands": {
            "staging": {
                "arso-weather": "",
                "dwd-current": "",
                "dwd-mosmix": "",
            },
        },
        "runitor": {
            "enabled": False,
            "ping": "",
        },
    }

    with config_file.open("w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    print_config_file(config_file)


def print_config_file(config_file: Path) -> None:
    """Print config file content."""
    if not config_file.exists():
        config_missing(config_file)

    with config_file.open() as f:
        config = yaml.safe_load(f)

    info_panel(
        yaml.dump(config, default_flow_style=False, sort_keys=False).strip("\n"),
        title=f"Configuration file: [bold]{config_file}[/bold]",
    )


def init_config(state: TyperState) -> Configuration:  # noqa: C901
    """Initialise configuration from CLI state."""
    if not state.config_file.exists():
        config_missing(state.config_file)

    with state.config_file.open() as f:
        config = yaml.safe_load(f)

    configuration = Configuration(state.config_file)
    if "logging" in config and "disabled" in config["logging"]:
        configuration.log_disabled = config["logging"]["disabled"]
    if (
        "logging" in config
        and "path" in config["logging"]
        and config["logging"]["path"]
    ):
        configuration.log_path = Path(config["logging"]["path"])
    if "default_mode" in config:
        configuration.database_type = DatabaseType(config["default_mode"])

    if "commands" in config:
        configuration.commands = config["commands"]
    if "runitor" in config:
        if "enabled" in config["runitor"]:
            configuration.runitor_enabled = config["runitor"]["enabled"]
        if "ping" in config["runitor"]:
            configuration.runitor_ping_url = config["runitor"]["ping"]

    if state:
        configuration.debug = state.debug
        if state.database_type:
            configuration.database_type = state.database_type

    configuration.mode = configuration.database_type.value

    if (
        "firebase" in config
        and configuration.mode in config["firebase"]
        and config["firebase"][configuration.mode]
    ):
        path = Path(config["firebase"][configuration.mode])
        if path.exists():
            configuration.firebase_credentials = path
            environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(path)

    info_panel(
        yaml.dump(
            configuration.to_object(),
            default_flow_style=False,
            sort_keys=False,
        ).strip("\n"),
        title="Configuration",
    )

    return configuration

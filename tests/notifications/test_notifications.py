"""Notifications tests."""

import pytest
from typer.testing import CliRunner

runner = CliRunner()


@pytest.mark.forked
def test_send_message(env: dict[str, str]) -> None:
    """Test send message."""
    from vremenar_utils.cli import application

    result = runner.invoke(
        application,
        ["send-message", "en_minor_DE058", "test message"],
        env=env,
        catch_exceptions=False,
    )
    assert result.exit_code == 0

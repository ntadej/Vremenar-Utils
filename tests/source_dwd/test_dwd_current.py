"""DWD current weather utilities tests."""

import pytest
from typer.testing import CliRunner

runner = CliRunner()


@pytest.mark.forked()
def test_current(env: dict[str, str]) -> None:
    """Test current weather update."""
    from vremenar_utils.cli import application

    result = runner.invoke(
        application,
        ["dwd-current", "--test-mode"],
        env=env,
        catch_exceptions=False,
    )
    assert result.exit_code == 0

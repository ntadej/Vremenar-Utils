"""ARSO maps utilities tests."""

import pytest
from typer.testing import CliRunner

runner = CliRunner()


@pytest.mark.forked()
def test_arso_maps(env: dict[str, str]) -> None:
    """Test ARSO maps update."""
    from vremenar_utils.cli import application

    result = runner.invoke(
        application,
        ["arso-maps"],
        env=env,
        catch_exceptions=False,
    )
    assert result.exit_code == 0

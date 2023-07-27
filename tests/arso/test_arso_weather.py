"""ARSO weather utilities tests."""
import pytest
from typer.testing import CliRunner

runner = CliRunner()


@pytest.mark.forked()
def test_arso_weather_main(env: dict[str, str]) -> None:
    """Test ARSO weather update."""
    from vremenar_utils.cli import application

    result = runner.invoke(
        application,
        ["arso-weather"],
        env=env,
        catch_exceptions=False,
    )
    assert result.exit_code == 0


@pytest.mark.forked()
def test_arso_weather_local_stations(env: dict[str, str]) -> None:
    """Test ARSO wearher update using local stations list."""
    from vremenar_utils.cli import application

    result = runner.invoke(
        application,
        ["arso-weather", "--local-stations"],
        env=env,
        catch_exceptions=False,
    )
    assert result.exit_code == 0

"""MeteoAlarm utilities tests."""
import pytest
from typer.testing import CliRunner

runner = CliRunner()


@pytest.mark.forked()
def test_alerts_germany(env: dict[str, str]) -> None:
    """Test alerts for Germany."""
    from vremenar_utils.cli import application

    result = runner.invoke(
        application,
        ["alerts-get", "de"],
        env=env,
        catch_exceptions=False,
    )
    assert result.exit_code == 0


@pytest.mark.forked()
def test_alerts_slovenia(env: dict[str, str]) -> None:
    """Test alerts for Slovenia."""
    from vremenar_utils.cli import application

    result = runner.invoke(
        application,
        ["alerts-get", "si"],
        env=env,
        catch_exceptions=False,
    )
    assert result.exit_code == 0

"""MeteoAlarm utilities tests."""
import pytest
from typer.testing import CliRunner

runner = CliRunner()


@pytest.mark.forked()
def test_alerts_areas_germany(env: dict[str, str]) -> None:
    """Test alerts areas for Germany."""
    from vremenar_utils.cli import application

    result = runner.invoke(
        application,
        [
            "alerts-areas",
            "de",
            "src/vremenar_utils/data/meteoalarm/de.json",
            "src/vremenar_utils/data/meteoalarm/de_stations.json",
        ],
        env=env,
        catch_exceptions=False,
    )
    assert result.exit_code == 0


@pytest.mark.forked()
def test_alerts_areas_slovenia(env: dict[str, str]) -> None:
    """Test alerts areas for Slovenia."""
    from vremenar_utils.cli import application

    result = runner.invoke(
        application,
        [
            "alerts-areas",
            "si",
            "src/vremenar_utils/data/meteoalarm/si.json",
            "src/vremenar_utils/data/meteoalarm/si_stations.json",
        ],
        env=env,
        catch_exceptions=False,
    )
    assert result.exit_code == 0


@pytest.mark.forked()
def test_alerts_get_germany(env: dict[str, str]) -> None:
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
def test_alerts_get_slovenia(env: dict[str, str]) -> None:
    """Test alerts for Slovenia."""
    from vremenar_utils.cli import application

    result = runner.invoke(
        application,
        ["alerts-get", "si"],
        env=env,
        catch_exceptions=False,
    )
    assert result.exit_code == 0


@pytest.mark.forked()
def test_alerts_notify_germany(env: dict[str, str]) -> None:
    """Test alerts notity for Germany."""
    from vremenar_utils.cli import application

    result = runner.invoke(
        application,
        ["alerts-notify", "de", "--dry-run"],
        env=env,
        catch_exceptions=False,
    )
    assert result.exit_code == 0


@pytest.mark.forked()
def test_alerts_notify_slovenia(env: dict[str, str]) -> None:
    """Test alerts notify for Slovenia."""
    from vremenar_utils.cli import application

    result = runner.invoke(
        application,
        ["alerts-notify", "si", "--dry-run"],
        env=env,
        catch_exceptions=False,
    )
    assert result.exit_code == 0


@pytest.mark.forked()
def test_alerts_update_germany(env: dict[str, str]) -> None:
    """Test alerts update for Germany."""
    from vremenar_utils.cli import application

    result = runner.invoke(
        application,
        ["alerts-update", "de", "--dry-run"],
        env=env,
        catch_exceptions=False,
    )
    assert result.exit_code == 0


@pytest.mark.forked()
def test_alerts_update_slovenia(env: dict[str, str]) -> None:
    """Test alerts update for Slovenia."""
    from vremenar_utils.cli import application

    result = runner.invoke(
        application,
        ["alerts-update", "si", "--dry-run"],
        env=env,
        catch_exceptions=False,
    )
    assert result.exit_code == 0

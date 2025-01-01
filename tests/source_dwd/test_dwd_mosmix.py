"""DWD MOSMIX utilities tests."""

import pytest
from typer.testing import CliRunner

runner = CliRunner()


@pytest.mark.forked
def test_mosmix_main(env: dict[str, str]) -> None:
    """Test MOSMIX update."""
    from vremenar_utils.cli import application

    result = runner.invoke(
        application,
        ["dwd-mosmix", "--test-mode"],
        env=env,
        catch_exceptions=False,
    )
    assert result.exit_code == 0


@pytest.mark.forked
def test_mosmix_local_source(env: dict[str, str]) -> None:
    """Test MOSMIX update from local source."""
    from vremenar_utils.cli import application

    result = runner.invoke(
        application,
        ["dwd-mosmix", "--local-source", "--test-mode"],
        env=env,
        catch_exceptions=False,
    )
    assert result.exit_code == 0


@pytest.mark.forked
def test_mosmix_local_stations(env: dict[str, str]) -> None:
    """Test MOSMIX update using local stations list."""
    from vremenar_utils.cli import application

    result = runner.invoke(
        application,
        ["dwd-mosmix", "--local-stations", "--test-mode"],
        env=env,
        catch_exceptions=False,
    )
    assert result.exit_code == 0

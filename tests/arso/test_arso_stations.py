"""ARSO stations utilities tests."""
import pytest

from typer.testing import CliRunner

runner = CliRunner()


@pytest.mark.forked
def test_stations_store(env: dict[str, str]) -> None:
    """Test stations store."""
    from vremenar_utils.cli import application  # type: ignore

    result = runner.invoke(
        application, ['stations-store', 'si'], env=env, catch_exceptions=False
    )
    assert result.exit_code == 0

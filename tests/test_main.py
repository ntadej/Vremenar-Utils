"""Main utilities tests."""
import pytest
from typer.testing import CliRunner

runner = CliRunner()


@pytest.mark.forked()
def test_help(env: dict[str, str]) -> None:
    """Test help."""
    from vremenar_utils.cli import application

    result = runner.invoke(application, ["--help"], env=env, catch_exceptions=False)
    assert result.exit_code == 0


@pytest.mark.forked()
def test_version(env: dict[str, str]) -> None:
    """Test version."""
    from vremenar_utils.cli import application

    result = runner.invoke(application, ["--version"], env=env, catch_exceptions=False)
    assert result.exit_code == 0

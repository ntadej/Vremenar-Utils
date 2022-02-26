"""DWD MOSMIX utilities tests."""
import pytest

from typer.testing import CliRunner

runner = CliRunner()


@pytest.mark.forked
def test_mosmix_main(env: dict[str, str]) -> None:
    """Test MOSMIX update."""
    from vremenar_utils.cli import application  # type: ignore

    result = runner.invoke(application, ['dwd-mosmix'], env=env, catch_exceptions=False)
    assert result.exit_code == 0

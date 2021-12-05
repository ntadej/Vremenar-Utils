"""DWD MOSMIX utilities tests."""
from typer.testing import CliRunner

from vremenar_utils.cli import application

runner = CliRunner()


def test_mosmix_main() -> None:
    """Test MOSMIX update."""
    result = runner.invoke(application, ['dwd-mosmix'])
    assert result.exit_code == 0

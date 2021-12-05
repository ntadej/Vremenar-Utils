"""DWD stations utilities tests."""
from tempfile import NamedTemporaryFile
from typer.testing import CliRunner

from vremenar_utils.cli import application

runner = CliRunner()


def test_stations() -> None:
    """Test stations update."""
    with NamedTemporaryFile(
        suffix='.csv', prefix='DWD_'
    ) as temporary_file, NamedTemporaryFile(
        suffix='.csv', prefix='NEW_DWD_'
    ) as temporary_file_new:
        print(temporary_file.name)
        print(temporary_file_new.name)
        result = runner.invoke(
            application, ['dwd-stations', temporary_file.name, temporary_file_new.name]
        )
        assert result.exit_code == 0

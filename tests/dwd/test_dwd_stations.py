"""DWD stations utilities tests."""
import pytest

from tempfile import NamedTemporaryFile
from typer.testing import CliRunner

runner = CliRunner()


@pytest.mark.forked
def test_stations(env: dict[str, str]) -> None:
    """Test stations update."""
    from vremenar_utils.cli import application  # type: ignore

    with NamedTemporaryFile(
        suffix='.csv', prefix='DWD_'
    ) as temporary_file, NamedTemporaryFile(
        suffix='.csv', prefix='NEW_DWD_'
    ) as temporary_file_new:
        print(temporary_file.name)
        print(temporary_file_new.name)
        result = runner.invoke(
            application,
            ['dwd-stations', temporary_file.name, temporary_file_new.name],
            env=env,
            catch_exceptions=False,
        )
        assert result.exit_code == 0


@pytest.mark.forked
def test_stations_store(env: dict[str, str]) -> None:
    """Test stations store."""
    from vremenar_utils.cli import application

    result = runner.invoke(
        application, ['stations-store', 'de'], env=env, catch_exceptions=False
    )
    print(result.stdout)
    print(result.exception)
    assert result.exit_code == 0

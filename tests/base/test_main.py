"""Main utilities tests."""

import pytest
from typer.testing import CliRunner

runner = CliRunner()


@pytest.mark.forked()
def test_help(env: dict[str, str]) -> None:
    """Test help."""
    from vremenar_utils.cli import application

    result = runner.invoke(application, ["--help"], env=env, catch_exceptions=False)

    print(result.stdout)  # noqa: T201
    assert result.exit_code == 0


@pytest.mark.forked()
def test_config_missing(env: dict[str, str]) -> None:
    """Test config missing."""
    from vremenar_utils.cli import application

    result = runner.invoke(application, ["config"], env=env, catch_exceptions=False)

    print(result.stdout)  # noqa: T201
    assert result.exit_code == 1


@pytest.mark.forked()
def test_version(env: dict[str, str]) -> None:
    """Test version."""
    from vremenar_utils.cli import application

    result = runner.invoke(application, ["--version"], env=env, catch_exceptions=False)

    print(result.stdout)  # noqa: T201
    assert result.exit_code == 0


@pytest.mark.forked()
def test_config_generate(env: dict[str, str]) -> None:
    """Test config generation."""
    from vremenar_utils.cli import application

    result = runner.invoke(
        application,
        ["config", "--generate"],
        env=env,
        catch_exceptions=False,
    )

    print(result.stdout)  # noqa: T201
    assert result.exit_code == 0


@pytest.mark.forked()
def test_config(env: dict[str, str]) -> None:
    """Test config."""
    from vremenar_utils.cli import application

    result = runner.invoke(application, ["config"], env=env, catch_exceptions=False)

    print(result.stdout)  # noqa: T201
    assert result.exit_code == 0

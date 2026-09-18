"""DWD Nominatim query tests."""

from importlib.resources import as_file, files

import pytest
from typer.testing import CliRunner

runner = CliRunner()


@pytest.mark.forked
def test_nominatim(env: dict[str, str]) -> None:
    """Test Nominatim query."""
    from vremenar_utils.cli import application

    # Clear cache
    from vremenar_utils.osm.nominatim import OSM_CACHE_DIR

    file_reverse = OSM_CACHE_DIR / "DWD:10015:reverse.json"
    file_details = OSM_CACHE_DIR / "DWD:10015:reverse_details.json"

    # Remove the cache files if they exist
    if file_reverse.is_file():
        file_reverse.unlink()
    if file_details.is_file():
        file_details.unlink()

    # Create a Traversable object pointing to the file
    resource_path = files("vremenar_utils").joinpath("data/stations/DWD.csv")

    # Context manager guarantees a physical path on disk
    with as_file(resource_path) as physical_path:
        # run without cache
        result = runner.invoke(
            application,
            ["dwd-osm-query", str(physical_path), "--test-mode"],
            env=env,
            catch_exceptions=False,
        )
        # run with cache
        result = runner.invoke(
            application,
            ["dwd-osm-query", str(physical_path), "--test-mode"],
            env=env,
            catch_exceptions=False,
        )
        assert result.exit_code == 0

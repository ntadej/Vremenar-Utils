"""DWD units tests."""


def test_units_conversion() -> None:
    """Test units conversion."""
    from vremenar_utils.dwd.units import (
        celsius_to_kelvin,
        hpa_to_pa,
        kelvin_to_celsius,
        km_to_m,
        kmh_to_ms,
        minutes_to_seconds,
    )

    assert abs(celsius_to_kelvin(31.4) - 304.55) < 1e-6
    assert abs(kelvin_to_celsius(300) - 26.85) < 1e-6
    assert hpa_to_pa(123) == 12300
    assert abs(km_to_m(12.3) - 12300) < 1e-6
    assert abs(kmh_to_ms(100) - 27.8) < 1e-6
    assert minutes_to_seconds(12) == 720


def test_units_mappings() -> None:
    """Test mappings."""
    from vremenar_utils.dwd.units import (
        _find,
        current_observations_weather_code_to_condition,
        synop_past_weather_code_to_condition,
    )

    assert synop_past_weather_code_to_condition(1) == "dry"
    assert synop_past_weather_code_to_condition(2) == "dry"
    assert synop_past_weather_code_to_condition(3) == "dry"
    assert synop_past_weather_code_to_condition(4) == "fog"
    assert synop_past_weather_code_to_condition(5) == "rain"

    assert current_observations_weather_code_to_condition(1) == "dry"
    assert current_observations_weather_code_to_condition(2) == "dry"
    assert current_observations_weather_code_to_condition(3) == "dry"
    assert current_observations_weather_code_to_condition(4) == "dry"
    assert current_observations_weather_code_to_condition(5) == "fog"

    assert _find({1: "a", 2: "b", 3: "c"}, 1) == "a"
    assert _find({1: "a", 2: "b", 3: "c"}, None) is None
    assert _find({}, 4) is None

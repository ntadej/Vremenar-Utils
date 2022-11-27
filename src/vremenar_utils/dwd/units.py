"""Parses of DWD open data."""
# Based on brightsky
# Copyright (c) 2020 Jakob de Maeyer


def celsius_to_kelvin(temperature: float) -> float:
    """Convert from Celsius to Kelvin."""
    return round(temperature + 273.15, 2)


def kelvin_to_celsius(temperature: float) -> float:
    """Convert from Kelvin to Celsius."""
    return round(temperature - 273.15, 2)


def hpa_to_pa(pressure: int | float) -> int:
    """Convert hPa to Pa."""
    return int(pressure * 100)


def km_to_m(distance: float) -> float:
    """Convert kilometres to metres."""
    return distance * 1000


def kmh_to_ms(speed: float) -> float:
    """Convert kilometres per hour to metres per second."""
    return round(speed / 3.6, 1)


def minutes_to_seconds(duration: int | float) -> int:
    """Convert minutes to seconds."""
    return int(duration * 60)


SYNOP_PAST_CONDITION_MAP: dict[int, str | None] = {
    0: 'dry',
    4: 'fog',
    5: 'rain',
    7: 'snow',
    8: 'rain',
    9: 'thunderstorm',
    10: 'dry',
    11: 'fog',
    14: 'rain',
    17: 'snow',
    18: 'rain',
    19: 'thunderstorm',
    20: None,
}

CURRENT_OBSERVATIONS_CONDITION_MAP: dict[int, str | None] = {
    1: 'dry',
    5: 'fog',
    7: 'rain',
    10: 'sleet',
    14: 'snow',
    18: 'rain',
    20: 'sleet',
    22: 'snow',
    26: 'thunderstorm',
    29: 'hail',
    31: 'dry',
    32: None,
}


def _find(mapping: dict[int, str | None], code: int | None) -> str | None:
    if code is None:
        return None
    value = None
    for k, v in mapping.items():
        if k > code:
            return value
        value = v
    return value


def synop_past_weather_code_to_condition(code: int) -> str | None:
    """DWD SYNOP code to condition."""
    return _find(SYNOP_PAST_CONDITION_MAP, code)


def current_observations_weather_code_to_condition(code: int) -> str | None:
    """DWD observations code to condition."""
    return _find(CURRENT_OBSERVATIONS_CONDITION_MAP, code)

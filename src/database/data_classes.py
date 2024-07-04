"""
This class contains enums, transfer objects and other data related artefacts.
"""
from collections import namedtuple

# Transfer object for all components
ComponentInfo = namedtuple('ComponentInfo', ['name', 'type', 'id', 'status', 'last_execution'])


# weather models
class Models:
    """Enum for the different types of weather models"""
    MOSMIX = "mosmix"
    ICON_D2 = "icon-d2"
    ICON_EU = "icon-eu"
    ICON_GLOBAL = "icon"
    ALL = [MOSMIX, ICON_D2, ICON_EU, ICON_GLOBAL]


class Measurements:
    """Enum for the different types of measurements"""
    PV_FORECAST = "pv_forecast"
    PV_MEASUREMENT = "pv_measurement"
    WEATHER_FORECAST = "weather_forecast"
    PV_EVALUATION = "pv_evaluation"
    ALL = [PV_FORECAST, PV_MEASUREMENT, WEATHER_FORECAST, PV_EVALUATION]

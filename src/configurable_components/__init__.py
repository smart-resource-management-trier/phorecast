"""
This module contains all components which are configurable, and saved in the database.
"""

from src.configurable_components.adapter import engine
from src.configurable_components.models.base_model import BaseModel
from src.configurable_components.models.mosmix_model import DWDMosmixModelLSTM
from src.configurable_components.target_loaders.base_target_loader import TargetLoader

from src.configurable_components.target_loaders.influx_target_loader import InfluxTargetLoader
from src.configurable_components.weather_loaders.base_weather_loader import WeatherLoader
from src.configurable_components.weather_loaders.dwd_mosmix_loader import DWDMosmixLoader
from src.utils.general import Base

target_loaders = {
    InfluxTargetLoader.__tablename__: InfluxTargetLoader
}

weather_loaders = {
    DWDMosmixLoader.__tablename__: DWDMosmixLoader
}

models = {
    DWDMosmixModelLSTM.__tablename__: DWDMosmixModelLSTM
}

configurable_components = {
    "target_loaders": target_loaders,
    "weather_loaders": weather_loaders,
    "models": models
}

base_classes = {
    "target_loaders": TargetLoader,
    "weather_loaders": WeatherLoader,
    "models": BaseModel
}

Base.metadata.create_all(engine)

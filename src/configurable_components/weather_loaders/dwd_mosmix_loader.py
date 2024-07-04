"""
This Module contains the loader for the DWD Mosmix weather data.
"""

import os
import re
import tempfile
from urllib import request
from urllib.error import URLError

import pandas as pd
from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from src.configurable_components import WeatherLoader
from src.configurable_components.exceptions import ComponentError
from src.configurable_components.weather_loaders.base_weather_loader import WeatherLoaderForm, Cell
from src.database.data_classes import Models, Measurements
from src.database.data_validation import DataValidationError
from src.database.influx_interface import influx_interface
from src.utils.dwd_tools import is_correct_mosmix_station, extract_kml_from_kmz, get_dwd_runid, \
    parse_kml_to_df, get_station_id
from src.utils.logging import get_default_logger
from src.utils.static import dwd_mosmix_parameters_file

logger = get_default_logger(__name__)


def get_mosmix_parameter_list() -> list[str]:
    """
    Get the list of parameters from the dwd mosmix parameters file
    :return: str list of parameters
    """
    resource = pd.read_csv(dwd_mosmix_parameters_file, sep=";")
    resource['parameter'] = resource['parameter'].str.strip()
    return resource['parameter'].values.tolist()


class DWDMosmixLoader(WeatherLoader):
    """
    This class represents a loader for DWD Mosmix weather data. Since data from the Mosmix model is
    not in grid format, the loader is based on the station ID. The station ID calculated by taking
    the minimum distance to the nearest station from the given coordinates.
    """

    MODEL = Models.MOSMIX
    FORM = WeatherLoaderForm
    __tablename__ = 'dwd_mosmix_weather_loader'
    __mapper_args__ = {"polymorphic_identity": "dwd_mosmix_weather_loader"}
    id: Mapped[int] = mapped_column(ForeignKey("weather_loader.id"), primary_key=True)
    station_id: Mapped[str] = mapped_column(String(10))

    # static parameters
    mosmix_parameters = get_mosmix_parameter_list()

    def _pre_execute(self):
        if not is_correct_mosmix_station(self.station_id):
            raise ValueError(f"Station ID {self.station_id} is not a valid mosmix station")

    def _execute(self):
        # retrieve file list from dwd server
        folder_url = (f"https://opendata.dwd.de/weather/local_forecasts/mos/MOSMIX_L/"
                      f"single_stations/{self.station_id}/kml/")

        try:
            files_html = request.urlopen(folder_url).read().decode('utf-8')
            files_list = re.findall(r'(?<=<a href=")(MOSMIX_L_\d{10}_\w*.kmz)', files_html)
            if not files_list:
                raise ValueError()
        except (URLError, ValueError) as e:
            raise ComponentError("Could not retrieve file list from dwd server", self) from e

        logger.debug("Retrieved file list from dwd server")
        # get existing forecasts
        existing_forecasts = influx_interface.get_existing_run_tags(
            measurement=Measurements.WEATHER_FORECAST, component_id=self.id)
        new_files = [x for x in files_list if get_dwd_runid(x) not in existing_forecasts]

        if new_files:
            logger.debug(f"Found {len(new_files)} new forecasts, to be added to the database")
        else:
            logger.info("No new forecasts found, nothing to do...")
            return False

        for file in new_files:
            with tempfile.TemporaryDirectory() as temp_dir:
                # download file and extract kml
                request.urlretrieve(folder_url + file, os.path.join(temp_dir, file))
                logger.debug(f"Downloading file: {file}, mosmix run not in database")

                kml_path = os.path.join(temp_dir, file[:-3] + "kml")
                extract_kml_from_kmz(os.path.join(temp_dir, file), kml_path)

                logger.debug(f"Extracted kml from file: {file}")

                # parse kml and write to database
                df = parse_kml_to_df(kml_path, DWDMosmixLoader.mosmix_parameters)

                logger.debug(f"Parsed kml to df: {file}")

                df.set_index("timestamp", inplace=True)

                run_id = get_dwd_runid(kml_path)

                try:
                    influx_interface.write_weather_forecast(df, DWDMosmixLoader.MODEL, run_id,
                                                            loader_id=self.id,
                                                            cell_id=self.cells[0].id)
                except DataValidationError as e:
                    raise ComponentError(f"Data validation failed on writing run {run_id}",
                                         self) from e

                logger.info(f"File extracted, parsed and written to db: {file}")
        return True

    @classmethod
    def from_form(cls, form: WeatherLoaderForm):
        lat = form.lat.data
        lon = form.lon.data
        station_id = get_station_id(lat, lon)
        cell = Cell(member=0, lat1=lat, lon1=lon, lat2=lat, lon2=lon)
        return cls(name=form.name.data, lat=lat, lon=lon, station_id=station_id, cells=[cell])

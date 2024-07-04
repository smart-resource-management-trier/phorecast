"""
This file contains the base class for all Weather Loaders, the form for the Loader,
as well as dummy class for testing.
"""
import time
from typing import List

import pandas as pd
from flask_wtf import FlaskForm
from sqlalchemy import ForeignKey, Float, Integer
from sqlalchemy.orm import mapped_column, Mapped, relationship
from wtforms import validators, FloatField, StringField, IntegerField

from src.configurable_components.adapter import ComponentInterface
from src.configurable_components.exceptions import ComponentError, log_component_error, \
    log_uncaught_error
from src.utils.general import Base
from src.utils.logging import get_default_logger

logger = get_default_logger(__name__)


class Cell(Base):
    """
    The cell class is there to store more than one time series for one location. E.g. an ensemble
    model which may have multiple predictions for one location and run or using multiple grid cells
    of a grid nwp model.

    The cell class is used so to not store all the metadata in influx, the influx entry is then
    matched by the cell id.

    :param loader_id: The id of the loader which is responsible for this cell
    :param loader: The loader which is responsible for this cell
    :param id: The id of the cell
    :param member: The member of the ensemble model if main run then 0 else set to the member number
    :param lat1: The latitude of the first point of the cell
    :param lon1: The longitude of the first point of the cell
    :param lat2: The latitude of the second point of the cell
    :param lon2: The longitude of the second point of the cell

    point one ist the upper left point of the cell and point two is the lower right point of the
    grid cell. This is to keep track of the position.
    """

    # relationships
    __tablename__ = 'cell'
    loader_id: Mapped[int] = mapped_column(ForeignKey("weather_loader.id"))
    loader: Mapped["WeatherLoader"] = relationship(back_populates="cells")

    # columns
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    member: Mapped[int] = mapped_column(Integer)
    lat1: Mapped[float] = mapped_column(Float)
    lon1: Mapped[float] = mapped_column(Float)
    lat2: Mapped[float] = mapped_column(Float, nullable=True)
    lon2: Mapped[float] = mapped_column(Float, nullable=True)


class WeatherLoaderForm(FlaskForm):
    """
    Base form for all WeatherLoaders
    """

    name = StringField('Name of the Weather Loader',
                       validators=[validators.DataRequired()])
    lat = FloatField('Latitude of the location to get the weather from',
                     validators=[validators.DataRequired(),
                                 validators.NumberRange(min=-90, max=90)])
    lon = FloatField('Longitude of the location to get the weather from',
                     validators=[validators.DataRequired(),
                                 validators.NumberRange(min=-180, max=180)])

    height = FloatField('Height of the location to get the weather from',
                        validators=[validators.Optional(),
                                    validators.NumberRange(min=0, max=10000)])


class WeatherLoader(Base, ComponentInterface):
    """
    Base form for all Weather Loaders
    """

    MODEL = None
    FORM = WeatherLoaderForm
    __tablename__ = 'weather_loader'
    __mapper_args__ = {
        "polymorphic_identity": "weather_loader",
        "polymorphic_on": "type",
    }

    # position info + weather model info
    lat: Mapped[float] = mapped_column(Float)
    lon: Mapped[float] = mapped_column(Float)
    height: Mapped[float] = mapped_column(Float, default=0)
    cells: Mapped[List["Cell"]] = relationship(back_populates="loader",
                                               cascade="all, delete")

    def run(self):
        """
        Main run method executable as thread, which calls the pre_execute, execute and post_execute
        methods in order, and catches any exceptions that might occur. It also logs the error in the
        database.
        """

        error = None
        try:
            self._pre_execute()
            self._execute()

        except ComponentError as e:
            logger.error(
                f"ComponentError in Weather Loader execution/pre_execution ({self.name}): {e}")
            log_component_error(e)
            error = type(e).__name__

        except Exception as e:  # pylint: disable=broad-except
            logger.error(
                f"Uncaught error in Weather Loader execution/pre_execution ({self.name}): "
                f"({type(e)})")
            logger.exception(e)
            log_uncaught_error(e, self)
            error = type(e).__name__

        finally:
            try:
                self._post_execute()

            except ComponentError as e:
                logger.error(f"ComponentError in Weather Loader post_execution ({self.name}): {e}")
                log_component_error(e)
                error = type(e).__name__

            except Exception as e:  # pylint: disable=broad-except
                logger.error(
                    f"Uncaught error in Weather Loader post_execution ({self.name}): ({type(e)})")
                logger.exception(e)
                log_uncaught_error(e, self)
                error = type(e).__name__

        self.error = error
        if self.error is None:
            self.last_execution = pd.Timestamp.utcnow()

    def _execute(self):
        """
        Method to be implemented by the subclass, which contains the actual execution logic
        - e.g. fetching data from an API
        - pre-processing the data
        - storing the data in the database
        :return: None
        """

    def _pre_execute(self):
        """
        Method to be implemented by the subclass, which contains the pre-execution logic
        - e.g. checking if the API is reachable
        - checking if the database connection is working
        - logging in
        :return: None
        """

    def _post_execute(self):
        """
        Method to be implemented by the subclass, which contains the post-execution logic
        - e.g. logging out
        - closing the database connection
        :return: None
        """


class DummyLoaderForm(WeatherLoaderForm):
    """
    This form is used to configure the DummyWeatherLoader
    """

    execution_time = IntegerField('Execution Time', default=30,
                                  validators=[validators.NumberRange(min=1)])


class DummyWeatherLoader(WeatherLoader):
    """
    Dummy loader for testing purposes
    """
    FORM = DummyLoaderForm
    __tablename__ = 'dummy_weather_loader'
    __mapper_args__ = {"polymorphic_identity": "dummy_weather_loader"}
    id: Mapped[int] = mapped_column(ForeignKey("weather_loader.id"), primary_key=True)
    execution_time: Mapped[int] = mapped_column(Integer, nullable=True)

    def _execute(self):
        logger.info("Dummy loader executed")
        time.sleep(self.execution_time * 0.2)

    def _pre_execute(self):
        logger.info("Dummy loader pre executed")
        time.sleep(self.execution_time * 0.6)

    def _post_execute(self):
        logger.info("Dummy loader post executed")
        time.sleep(self.execution_time * 0.2)

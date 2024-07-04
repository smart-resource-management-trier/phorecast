"""
Base model class for all models
"""

import os.path
import secrets
import time
from datetime import datetime
from typing import List

import pandas as pd
from flask_wtf import FlaskForm
from sqlalchemy import String, ForeignKey, Float, DateTime, Integer
from sqlalchemy.orm import Mapped, mapped_column, relationship
from wtforms import StringField, validators, SelectField, IntegerField

from src.configurable_components.adapter import Session, ComponentInterface
from src.configurable_components.exceptions import ComponentError, log_component_error, \
    log_uncaught_error
from src.configurable_components.target_loaders.base_target_loader import Field
from src.configurable_components.weather_loaders.base_weather_loader import WeatherLoader
from src.database.influx_interface import influx_interface
from src.utils.general import Base
from src.utils.logging import get_default_logger
from src.utils.static import model_data_path

logger = get_default_logger(__name__)


def get_target_choices():
    """
    Returns all fields that can be used as target fields
    :return: list of (id, name) tuples
    """
    with Session.begin() as session:
        target_fields = session.query(Field).all()
        return [(field.id, field.influx_field) for field in target_fields]


def get_loader_choices(loader_type: [str]):
    """
    Returns all weather loaders that can be used as input loaders
    :param loader_type: list of the table name of the allowed loaders
    :return: list of (id, name) tuples
    """
    with Session.begin() as session:
        target_fields = session.query(WeatherLoader).all()
        return [(loader.id, f'{loader.type}: {loader.name}') for loader in target_fields if
                loader.type in loader_type]


class ModelRun(Base):
    """
    ModelRun class for storing the runs of the models
    :param path: path to the model run folder
    :param loss: loss or other error metric of the model run
    :param ts_start: timestamp of the start of the model run
    """
    __tablename__ = 'model_run'

    # I dont understand why but pylint does not get that Mapped is a type (it does it no where else)
    # pylint: disable=unsubscriptable-object
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    path: Mapped[str] = mapped_column(String(500), nullable=True)
    loss: Mapped[float] = mapped_column(Float, nullable=True)
    ts_start: Mapped[datetime] = mapped_column(DateTime, default=pd.Timestamp.utcnow)

    model_id: Mapped[int] = mapped_column(ForeignKey("model.id"))
    model: Mapped["BaseModel"] = relationship("BaseModel", back_populates="runs")


class ModelForm(FlaskForm):
    """Form for the Model class"""
    name = StringField('Name of the Model',
                       validators=[validators.DataRequired()])

    field_id = SelectField('Select a target field to learn and predict')

    loader_id = SelectField(
        "Choose a weather loader to get the input data from "
        "(the loaction of the weather loader will be used)")


class BaseModel(Base, ComponentInterface):
    """
    Base model class for all models, a model is a entity which predicts a target field based on
    weather data this is done by training the model on the weather data (train method) and then
    predicting new data with the (predict method).
    """

    FORM = ModelForm
    INPUT_LOADERS = None

    __tablename__ = 'model'
    __mapper_args__ = {
        "polymorphic_identity": "model",
        "polymorphic_on": "type",
    }
    runs: Mapped[List["ModelRun"]] = relationship("ModelRun", back_populates="model",
                                                  cascade="all, delete")

    target_field: Mapped["Field"] = relationship("Field")
    field_id: Mapped[int] = mapped_column(ForeignKey("field.id"), nullable=True)

    source_loader: Mapped["WeatherLoader"] = relationship("WeatherLoader")
    loader_id: Mapped[int] = mapped_column(ForeignKey("weather_loader.id"), nullable=True)

    def execute(self):
        """
        Executes the model by calling the train and predict methods and handling the errors.
        """

        error = None
        try:
            if self.retrain:
                self.train()
        except ComponentError as e:
            logger.error(f"ComponentError in Model training ({self.name}): {e}")
            log_component_error(e)
            error = type(e).__name__
        except Exception as e:  # pylint: disable=broad-except
            logger.error(f"Uncaught error in Model training"
                         f"({self.name}): ({type(e)})")
            logger.exception(e)
            log_uncaught_error(e, self)
            error = type(e).__name__

        try:
            self.predict()
        except ComponentError as e:
            logger.error(f"ComponentError in Model prediction ({self.name}): {e}")
            log_component_error(e)
            error = type(e).__name__
        except Exception as e:  # pylint: disable=broad-except
            logger.error(f"Uncaught error in Model prediction"
                         f"({self.name}): ({type(e)})")
            logger.exception(e)
            log_uncaught_error(e, self)
            error = type(e).__name__

        self.error = error
        if self.error is None:
            self.last_execution = pd.Timestamp.utcnow()

    def train(self):
        """
        this method should be overwritten by the child classes and consist of all logic for
        fitting a model to the data this includes:

        - loading the train data
        - deliberation to train
        - preprocessing of the data
        - training the model
        - saving the model and metadata
        """

    def predict(self):
        """
        this method should be overwritten by the child classes and consist of all logic for
        predicting data with a trained model:

        - checking for missing runs
        - loading a trained model
        - preprocessing of the data
        - predicting the data
        - saving the results to influx
        """

    def create_new_run_dir(self) -> str:
        """
        Returns the directory where the model run is stored
        :return: path to the model directory
        """
        random_folder = secrets.token_hex(8)

        path = os.path.join(model_data_path, f"{self.name}_{self.id}", random_folder)

        if os.path.exists(path):
            raise FileExistsError(f"Could not make model run directory: {path} already exists")

        os.makedirs(path)

        return path

    def get_best_run(self, best_of: int = None) -> ModelRun | None:
        """
        Returns the run with the lowest loss
        :param best_of: if set, returns the best last n runs
        :return: ModelRun object
        """
        if not self.runs:
            return None

        if best_of is not None:
            return min(sorted(self.runs, key=lambda x: x.ts_start, reverse=True)[:best_of],
                       key=lambda x: x.loss)

        return min(self.runs, key=lambda x: x.loss)

    @property
    def retrain(self) -> bool:
        """
        Checks if the model should be retrained
        :return: bool
        """
        last_run = self.last_run
        if (last_run is not None and last_run.ts_start > pd.Timestamp.utcnow().tz_localize(None) -
                pd.Timedelta(days=7)):
            logger.debug("Model was already trained in the last 7 days, skipping retraining")
            return False

        logger.debug("Model needs to be retrained, since last training was more than 7 days ago")
        return True

    @property
    def train_data(self) -> pd.DataFrame:
        """
        Retrieves Train data from the source loader and returns it as a pandas dataframe
        :return: DataFrame with datetime index
        """
        data = influx_interface.get_training_examples(
            self.source_loader.cells[0].id,
            self.target_field.influx_field
        )

        return data

    @property
    def missing_runs(self) -> [int]:
        """
        Returns the run ids of the missing runs
        :return: list of run ids
        """
        return influx_interface.get_missing_forecast_ids(self.source_loader.id, self.id)

    @property
    def last_run(self) -> ModelRun | None:
        """
        Returns the last run of the model
        :return: ModelRun object
        """
        if not self.runs:
            return None

        return max(self.runs, key=lambda x: x.ts_start)

    @classmethod
    def get_form(cls, obj=None) -> FlaskForm:
        """
        Returns a form for the model
        :param obj: object for form prefill
        :return:
        """
        if obj:
            form = cls.FORM(obj=obj)
        else:
            form = cls.FORM()

        # dynamically set the choices for the select fields
        form.loader_id.choices = get_loader_choices(cls.INPUT_LOADERS)
        form.field_id.choices = get_target_choices()

        return form


class DummyModelForm(ModelForm):
    """
    Form for the DummyModel
    """

    execution_time = IntegerField('Execution Time', default=30,
                                  validators=[validators.NumberRange(min=1)])


class DummyModel(BaseModel):
    """
    Dummy model for testing purposes
    This model only has the bare minimum of functionality implemented with a sleep time to simulate
    the execution.
    """
    # I dont understand why but pylint does not get that Mapped is a type (it does it no where else)
    # pylint: disable=unsubscriptable-object

    FORM = DummyModelForm
    __tablename__ = 'dummy_model'
    __mapper_args__ = {"polymorphic_identity": "dummy_model"}
    id: Mapped[int] = mapped_column(ForeignKey("model.id"), primary_key=True)
    execution_time: Mapped[int] = mapped_column(Integer)

    def train(self):
        logger.info("Dummy model trained")
        time.sleep(self.execution_time * 0.8)

    def predict(self):
        logger.info("Dummy model predicted")
        time.sleep(self.execution_time * 0.2)

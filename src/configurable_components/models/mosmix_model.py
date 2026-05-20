"""
This module contains the DWD Mosmix LSTM model class and form. The model uses timeseries data to
predict the output utilizing a LSTM neural network. It also contains the form for the model.
"""

import os
import random

import keras
import numpy as np
import pandas as pd
import phorecast_ml
from sqlalchemy import ForeignKey, Float, Integer
from sqlalchemy.orm import mapped_column, Mapped
from wtforms import IntegerField, validators

from src.configurable_components.exceptions import ComponentError
from src.configurable_components.models.base_model import ModelForm, BaseModel, ModelRun
from src.database.influx_interface import influx_interface
from src.utils.general import plot_history, plot_windows, plot_predictions
from src.utils.logging import get_default_logger

logger = get_default_logger(__name__)


class DWDMosmixModelForm(ModelForm):
    """
    Form for the DWD Mosmix LSTM model.
    """

    window_size = IntegerField('Window Size: Size of the time window in hours', default=24,
                               validators=[validators.NumberRange(min=1, max=100)])
    factor_width = IntegerField('Factor for sizing the NNs width', default=3,
                                validators=[validators.NumberRange(min=1)])
    factor_depth = IntegerField('Factor for sizing the NNs depth', default=5,
                                validators=[validators.NumberRange(min=1)])
    batch_size = IntegerField('Training batch size', default=32,
                              validators=[validators.NumberRange(min=1)])
    epochs = IntegerField('duration of training (epochs)', default=300,
                              validators=[validators.NumberRange(min=1)])

class DWDMosmixModelLSTM(BaseModel):
    """
    This class represents a LSTM model for the DWD Mosmix weather data, it uses timeseries data to
    predict the output utilizing a LSTM neural network. Preprocessing is done by filtering the data,
    adding solar positions and removing outliers with fixed rules and a linear regression model.
    It trains the model every 7 days.
    """
    FORM = DWDMosmixModelForm
    INPUT_LOADERS = ["dwd_mosmix_weather_loader"]

    PARAMETERS = ["TTT", "Td", "DD", "FF", "FX1", "RR1c", "RRS1c", "N", "Neff", "N05",
                  "Nl", "Nm", "Nh", "PPPP", "T5cm", "Rad1h", "VV", "SunD1", "wwM", "DRR1", "wwZ",
                  "wwD", "wwC", "wwT", "wwL", "wwS", "wwF", "wwP", "VV10", "R101", "R102", "R103",
                  "R105", "R107", "R110", "R120", "RRad1", "R130", "R150", "RR1o1", "RR1w1",
                  "RR1u1", "RRL1c", "Nlm", "azimuth", "elevation", "zenith"]
    TRAIN_TEST_SPLIT = 0.25

    __tablename__ = 'dwd_mosmix_model_lstm'
    __mapper_args__ = {"polymorphic_identity": "dwd_mosmix_model_lstm"}
    id: Mapped[int] = mapped_column(ForeignKey("model.id"), primary_key=True)

    window_size: Mapped[int] = mapped_column(Integer)
    factor_width: Mapped[float] = mapped_column(Float)
    factor_depth: Mapped[int] = mapped_column(Integer)
    batch_size: Mapped[int] = mapped_column(Integer)
    epochs: Mapped[int] = mapped_column(Integer)

    def train(self):
        """
        This method trains the LSTM model for the DWD Mosmix weather data. It first checks if there
        is enough training data. If not, it raises an error. Then, it preprocesses the data, creates
        windows from the dataset, and splits the windows into training and testing sets. It then
        trains the model with callbacks, saves the model, and plots the training history,
        windows, and test predictions.
        """

        # Retrieve the training data
        try:
            data = self.train_data
        except ValueError as e:
            raise ComponentError("There seems to be no training data to train on", self) from e

        # check for the amount of actual training data
        days_in_training_data = len(data) / 24
        if days_in_training_data < 30:
            logger.warning(f"Training data only contains {days_in_training_data:.0f} "
                           f"days worth of samples minimum is 30 days, model cant be trained")
            raise ComponentError(f"Not enough training data available: "
                                 f"has {days_in_training_data} days should be 30", self)
        logger.info(
            f"Retrieved Training data with {days_in_training_data:.0f} days worth of samples")

        # Preprocess the data
        data = phorecast_ml.preprocessing.solar.attach_solar_positions(data, self.source_loader.lat, self.source_loader.lon, self.source_loader.height)
        data = phorecast_ml.preprocessing.filtering.solar_position_filter(data,["elevation", "azimuth", "Rad1h"], self.target_field.influx_field)

        # Create windows from the dataset and split them into test and train
        try:
            windows = phorecast_ml.preprocessing.windowing(data, 24, 24 // 4, 2)
        except ValueError as e:
            raise ComponentError("Could not create windows from dataset, "
                                 "maybe the data is to cut up or noisy", self) from e

        train_test_split = DWDMosmixModelLSTM.TRAIN_TEST_SPLIT
        train_windows, test_windows = phorecast_ml.preprocessing.dataset_splitting.split_windows(windows, test_ratio=0.24, factor=14)

        # check the test ratio
        actual_test_ratio = len(test_windows) / (len(test_windows) + len(train_windows))
        if abs(actual_test_ratio - train_test_split) > 0.1:
            logger.warning(f"Test ratio is {actual_test_ratio:.2f} "
                           f"instead of {train_test_split:.2f} , training anyways")
        else:
            logger.info(f"Training model with {len(train_windows)} train windows and testing with "
                        f"{len(test_windows)} windows (window_size: {self.window_size})")

        # for performance reasons, the data is converted into tf datasets
        (train_X, train_y), _ = phorecast_ml.preprocessing.get_dataset_from_windows(
            train_windows, target=self.target_field.influx_field)
        (test_X, test_y), test_index = phorecast_ml.preprocessing.get_dataset_from_windows(
            test_windows, target=self.target_field.influx_field)

        units = int(self.factor_width * len(self.PARAMETERS))
        depth = int(self.factor_depth * len(self.PARAMETERS))

        model = phorecast_ml.model.LSTM(epochs=self.epochs,
                                        units=units,
                                        depth=depth,
                                        metrics=["mean_absolute_error", ],
                                        batch_size=self.batch_size)
        history = model.train(train_X, train_y, test_X, test_y)

        # Save the model and complete the run
        best_loss = min(history.history['val_loss'])
        path = self.create_new_run_dir()

        run = ModelRun(path=path, loss=best_loss)
        self.runs.append(run)

        model.save(os.path.join(run.path, "model.keras"))
        logger.info(f"Successfully trained {self.name} model with loss of {best_loss} ")

        # plot trainings history and windows
        plot_history(history, os.path.join(run.path, "history.jpeg"))
        plot_windows(train_windows, test_windows, os.path.join(run.path, "train_windows.jpeg"))

        # plot 3 test predictions
        test_predictions = model.predict(test_X)
        for _ in range(3):
            index = random.randint(0, len((test_X, test_y)[0])-1)
            df = pd.DataFrame({'label': (test_X, test_y)[1][index].reshape(-1),
                               'prediction': test_predictions[index].reshape(-1),
                               'reference': [x[15] for x in (test_X, test_y)[0][index]]},
                              index=test_index[index])
            plot_predictions(df, os.path.join(run.path, f"example_{index}.jpeg"), "GLI")

    def predict(self):
        if not self.runs:
            logger.info(f"Model {self.name} has no runs, can't predict")
            return

        missing_runs = self.missing_runs
        if not missing_runs:
            return

        # nothing to predict

        best_run = self.get_best_run()

        logger.info(f"Loading model from run {best_run.id} with loss {best_run.loss}")

        model = keras.models.load_model(os.path.join(best_run.path, "model.keras"))
        for run in missing_runs:
            # reset all LSTM states
            for layer in model.layers:
                if isinstance(layer, keras.layers.LSTM):
                    layer.reset_states()

            # prepare the data
            input_df = influx_interface.get_weather_forecasts(loader_id=self.source_loader.id,
                                                              run=run)
            input_df = input_df.drop("model", axis=1)
            input_df = phorecast_ml.preprocessing.solar.attach_solar_positions(input_df, self.source_loader.lat, self.source_loader.lon, self.source_loader.height)
            input_df = phorecast_ml.preprocessing.filtering.solar_position_filter(input_df,
                                                                       ["elevation", "azimuth", "Rad1h"],
                                                                       self.target_field.influx_field)

            windows = phorecast_ml.preprocessing.windowing(input_df, self.window_size, self.window_size, self.window_size)
            (data, _), _ = phorecast_ml.preprocessing.get_dataset_from_windows(
                windows, target=self.target_field.influx_field)

            # predict the data
            predictions = model.predict(data, batch_size=1, verbose=0)

            # reformat the predictions (clip values to 0, inf) attach them to the input_df
            predictions = predictions.reshape(-1)
            input_df[self.target_field.influx_field] = (
                np.clip(predictions[:len(input_df)], 0, None))
            prediction_df = input_df[[self.target_field.influx_field]]

            # write the predictions to the database
            influx_interface.write_pv_forecast(prediction_df, self.id, run)
            logger.info(f"Model {self.name} created predictions for run {run}")

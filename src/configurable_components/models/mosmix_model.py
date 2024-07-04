"""
This module contains the DWD Mosmix LSTM model class and form. The model uses timeseries data to
predict the output utilizing a LSTM neural network. It also contains the form for the model.
"""

import os
import random
import tempfile

import keras
import numpy as np
import pandas as pd
import tensorflow as tf
from sklearn.linear_model import LinearRegression
from sqlalchemy import ForeignKey, Float, Integer
from sqlalchemy.orm import mapped_column, Mapped
from wtforms import IntegerField, validators

from src.configurable_components.exceptions import ComponentError
from src.configurable_components.models.base_model import ModelForm, BaseModel, ModelRun
from src.database.influx_interface import influx_interface
from src.utils.dataset import attach_solar_positions, windowing, get_dataset_from_windows, \
    split_windows, create_tf_dataset
from src.utils.general import plot_history, plot_windows, plot_predictions
from src.utils.keras import sum_difference_metric
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
        data = self._preprocessing(data)

        # Create windows from the dataset and split them into test and train
        try:
            windows = windowing(data, self.window_size, self.window_size // 4, 2)
        except ValueError as e:
            raise ComponentError("Could not create windows from dataset, "
                                 "maybe the data is to cut up or noisy", self) from e
        train_test_split = DWDMosmixModelLSTM.TRAIN_TEST_SPLIT
        train_windows, test_windows = split_windows(windows, train_test_split, factor=14)

        # check the test ratio
        actual_test_ratio = len(test_windows) / (len(test_windows) + len(train_windows))
        if abs(actual_test_ratio - train_test_split) > 0.1:
            logger.warning(f"Test ratio is {actual_test_ratio:.2f} "
                           f"instead of {train_test_split:.2f} , training anyways")
        else:
            logger.info(f"Training model with {len(train_windows)} train windows and testing with "
                        f"{len(test_windows)} windows (window_size: {self.window_size})")

        # for performance reasons, the data is converted into tf datasets
        train_ds_np, _ = get_dataset_from_windows(train_windows,
                                                  target=self.target_field.influx_field)
        test_ds_np, test_index = get_dataset_from_windows(test_windows,
                                                          target=self.target_field.influx_field)

        train_ds = create_tf_dataset(train_ds_np, self.batch_size, shuffle=True)
        test_ds = create_tf_dataset(test_ds_np, self.batch_size)

        # Train the model with callbacks
        with tempfile.TemporaryDirectory() as temp_dir:
            keras.config.disable_traceback_filtering()
            model = self._build_lstm(train_ds)

            temp_file = os.path.join(temp_dir, "model_temp.keras")
            checkpoint_callback = keras.callbacks.ModelCheckpoint(temp_file,
                                                                  monitor="val_loss", mode="min",
                                                                  save_best_only=True,
                                                                  verbose=0)
            early_stopping_callback = keras.callbacks.EarlyStopping(patience=30)

            history = model.fit(train_ds, epochs=300, validation_data=test_ds,
                                callbacks=[checkpoint_callback, early_stopping_callback], verbose=0)

            model = keras.models.load_model(temp_file)

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
        test_predictions = model.predict(test_ds, verbose=0)
        for _ in range(3):
            index = random.randint(0, len(test_ds_np[0]))
            df = pd.DataFrame({'label': test_ds_np[1][index].reshape(-1),
                               'prediction': test_predictions[index].reshape(-1),
                               'reference': [x[15] for x in test_ds_np[0][index]]},
                              index=test_index[index])
            plot_predictions(df, os.path.join(run.path, f"example_{index}.jpeg"), "GLI")

    def predict(self):
        if not self.runs:
            logger.info(f"Model {self.name} has no runs, can't predict")
            return

        missing_runs = self.missing_runs
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
            input_df = self._preprocessing(input_df)
            windows = windowing(input_df, self.window_size, self.window_size, self.window_size)
            (data, _), _ = get_dataset_from_windows(windows)

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

    def _preprocessing(self, data):
        """
        THis function filters the data in 3 steps and adds solar positions to it
        :param data: dataframe with the data
        :return: dataframe with the filtered data
        """

        lat = self.source_loader.lat
        lon = self.source_loader.lon
        height = self.source_loader.height
        target = self.target_field.influx_field

        data = attach_solar_positions(data, lat, lon, height)
        original_length = len(data)

        missing_cols = [x for x in self.PARAMETERS if x not in
                        data.columns]

        if len(missing_cols) > 0:
            logger.warning(f"Training data preprocessing: Missing columns in data: {missing_cols}")

        for m in missing_cols:
            data[m] = 0

        if target not in data.columns:
            return data[self.PARAMETERS]

        # Rule 1: If elevation < -10, target has to be 0
        data = data[~((data['elevation'] < -10) & (data[target] > 0))].copy()
        rule_1_count = original_length - len(data)

        # Rule 2: Remove rows where elevation > 10 and target is 0
        data = data[~((data['elevation'] > 10) & (data[target] == 0))].copy()
        rule_2_count = (original_length - len(data)) - rule_1_count
        # Rule 3: Remove outliers with Linear Regression
        x = data[['elevation', 'azimuth', 'Rad1h']]  # Independent variable
        y = data[self.target_field.influx_field]  # Dependent variable

        # Fit the model and predict
        model = LinearRegression()
        model.fit(x, y)
        y_pred = model.predict(x)

        # calculate residuals
        data['__residuals'] = y - y_pred
        residual_std = np.std(data['__residuals'])
        residual_mean = np.mean(data['__residuals'])
        threshold = 5 * residual_std

        condition = (data['__residuals'] <= (residual_mean + threshold)) & (
                data['__residuals'] >= (residual_mean - threshold))

        data_filtered = data.loc[condition].copy()
        rule_3_count = original_length - len(data_filtered) - rule_1_count - rule_2_count

        logger.info(
            f"Removed {(rule_1_count / original_length) * 100:.2f}% "
            f"of rows with elevation < -10 and target > 0")
        logger.info(
            f"Removed {(rule_2_count / original_length) * 100:.2f}% "
            f"of rows with elevation > 10 and target = 0")
        logger.info(
            f"Removed {(rule_3_count / original_length) * 100:.2f}% "
            f"of rows with Linear Regression")

        data_filtered.drop(columns=['__residuals'], inplace=True)

        return data_filtered[[*self.PARAMETERS, target]]

    def _build_lstm(self, data: tf.data.Dataset) -> keras.models.Sequential:
        """
        Builds a LSTM model with the given hyperparameters
        :param data: give data to adapt the normalization layer
        :return: build and fitted keras model
        """
        # define model hyperparameters
        learning_rate = 0.001
        loss = keras.losses.MeanAbsoluteError()
        features = len(self.PARAMETERS)
        width = int(features * self.factor_width)
        model = keras.models.Sequential()
        batch_size = self.batch_size if data is not None else 1

        # create input (InputLayer, MaskingLayer, NormalizationLayer)
        model.add(keras.layers.Input((100, features), batch_size=batch_size))
        model.add(keras.layers.Masking(mask_value=0))
        # create and adapt normalization layer
        normalization_layer = keras.layers.Normalization(axis=-1)
        model.add(normalization_layer)
        normalization_layer.adapt(data=data.map(lambda x, y: x))

        # create body of model (LSTM layers)
        for _ in range(self.factor_depth):
            model.add(keras.layers.LSTM(units=width,
                                        return_sequences=True,
                                        dropout=0.2))

        # create output layer (DenseLayer, inverted NormalizationLayer)
        model.add(keras.layers.Dense(units=1, activation='linear'))
        # create and adapt normalization layer
        normalization_layer_output = keras.layers.Normalization(axis=-1, invert=True)
        model.add(normalization_layer_output)
        normalization_layer_output.adapt(data=data.map(lambda x, y: y))

        # compile model
        model.compile(optimizer=keras.optimizers.Adam(learning_rate), loss=loss,
                      metrics=['mean_absolute_error', sum_difference_metric])

        logger.debug(f"Built LSTM model with width: {width}, depth: {self.factor_depth}, "
                     f"batch_size: {batch_size}")

        return model

"""
In this module methods for data validation are implemented. Each taking a pandas dataframe as
input and returning a boolean value. true for correct data and false for incorrect data.
"""
import datetime

import pandas as pd

from src.database.data_classes import Models


class DataValidationError(Exception):
    """Base class for data validation exceptions"""
    def __init__(self, message: str = "Data in DataFrame is invalid"):
        """Exception raised for errors in a dataframe in the validation process."""
        super().__init__(message)


class DataValidationFaultyField(DataValidationError):
    """Exception raised if a field in a DataFrame is faulty"""
    def __init__(self, message: str = "Data in DataFrame has a wrong or faulty field"):
        """
        :param message: message to display
        """
        super().__init__(message)


class DataValidationFaultyIndex(DataValidationError):
    """Exception raised for errors in the index of a dataframe"""
    def __init__(self, message: str = "Data in DataFrame has a wrong or faulty tag"):
        """
        :param message: message to display
        """
        super().__init__(message)


class DataValidationFaultyTag(DataValidationError):
    """Exception raised for if DataFrame has a missing or faulty tag"""
    def __init__(self, message: str = "Data in DataFrame has a missing or faulty tag"):
        """
        :param message: message to display
        """
        super().__init__(message)


def basic_validation(data: pd.DataFrame):
    """
    This method checks the general structure of the input data
    :param data: dataframe to check
    raises DataValidationError if data is faulty
    """

    # check if data is a dataframe
    if not isinstance(data, pd.DataFrame):
        raise DataValidationError("Data is not a DataFrame")

    # check if data has at least one row
    if len(data) < 1:
        raise DataValidationError("Data has no rows")

    # check if data has at least one column
    if len(data.columns) < 1:
        raise DataValidationError("Data has no columns")

    # check if data has a datetime index
    if not isinstance(data.index, pd.DatetimeIndex):
        raise DataValidationFaultyIndex("Data index is not a datetime index")

    # check if data has no NaN values
    if data.isna().any().any():
        raise DataValidationError("Data has NaN values")

    # data has to have an api version tag
    if "api_version" not in data.columns:
        raise DataValidationFaultyTag("missing api version tag column")

    if data["api_version"].nunique() != 1:
        raise DataValidationFaultyTag("PV source tag should be a constant value")


def hourly_validation(data: pd.DataFrame, distinct: bool = False):
    """
    This method checks the hourly structure of the input data
    :param distinct: defines if the data should be checked for distinct timestamps
    :param data: dataframe to check
    raises DataValidationError if data is faulty
    """

    sorted_data = data.sort_index()

    # check if data is hourly indexed
    full_hour_check = all(sorted_data.index.minute == 0) and all(sorted_data.index.second == 0)

    if not full_hour_check:
        raise DataValidationFaultyIndex("Data is not hourly indexed")

    if not distinct:
        return

    # check if data has no duplicates
    if sorted_data.index.duplicated().any():
        raise DataValidationFaultyIndex("Data has duplicate timestamps")

    # check if data has no missing hours
    diff = sorted_data.index.to_series().diff().dropna().tolist()

    for d in diff:
        if d != pd.Timedelta("1h"):
            raise DataValidationFaultyIndex("Data has missing hours")


def run_tag_validation(data: pd.DataFrame):
    """
    This method checks the run tag of the input data
    :param data: dataframe to check
    raises DataValidationError if data is faulty
    """

    if "run" not in data.columns:
        raise DataValidationFaultyTag("Data has no run tag column")

    for x in data["run"]:
        if len(str(x)) != 10:
            raise DataValidationFaultyTag("Run tag has to be in YYYYMMDDHH format")


def validate_pv_data(data: pd.DataFrame):
    """
    This is the validation method for PV data which should follow the following structure:
    - hourly continuous datetime index
    TAGS:
    - loader_id: constant value indicating the source of the data (loader name)
    VALUES:
    - at least 1 field column

    :param data: dataframe to check
    raises DataValidationError if data is faulty
    """
    basic_validation(data)
    hourly_validation(data, distinct=False)

    if "loader_id" not in data.columns:
        raise DataValidationFaultyTag("PV data has no source tag column")

    if data["loader_id"].nunique() != 1:
        raise DataValidationFaultyTag("PV source tag should be a constant value")

    if len(data.columns) < 3:
        raise DataValidationFaultyField("PV data has to hav at least 1 field columns")


def validate_weather_forecast(data: pd.DataFrame):
    """
    This is the validation method for weather data which should follow the following structure:
    - hourly continuous datetime index
    TAGS:
    - model: name of the weather model
    - run: ZULU time of the model run in YYYYMMDDHH format
    - loader_id: ID of loader the data is based on
    - cell_id: Additional ID to make ensemble models or grid cell models identifiable
    if the model is a grid cell model like any icon model

    VALUES:
    - 1 field for each weather parameter of the model, field names should match the model parameter
    names

    :param data: dataframe to check
    raises DataValidationError if data is faulty
    """

    basic_validation(data)
    hourly_validation(data, distinct=False)
    run_tag_validation(data)

    if "loader_id" not in data.columns:
        raise DataValidationFaultyTag("Weather data has no source tag column")

    if data["loader_id"].nunique() != 1:
        raise DataValidationFaultyTag("Weather source tag should be a constant value")

    if "model" not in data.columns:
        raise DataValidationFaultyTag("Weather data has no model tag column")

    for x in data["model"]:
        if x not in Models.ALL:
            raise DataValidationFaultyTag("Weather model tag has to be one of the following: "
                                          f"{Models.ALL}, was {x}")

    if "cell_id" not in data.columns:
        raise DataValidationFaultyTag("Weather data has no cell id tag column")

    if len(data.columns) < 6:
        raise DataValidationFaultyField("Weather data has to have at least 1 field columns")


def validate_pv_forecast(data: pd.DataFrame):
    """
    This is the validation method for PV forecast data which should follow the following structure:
    TAGS:
    - run: ZULU time of the model run in YYYYMMDDHH format
    - model_id : ID of the model

    VALUES:
    - 1 prediction named like a pv measurement

    :param data: dataframe to check
    raises DataValidationError if data is faulty
    """

    basic_validation(data)
    hourly_validation(data, distinct=True)
    run_tag_validation(data)

    if "model_id" not in data.columns:
        raise DataValidationFaultyTag("PV forecast data has no model id tag column")

    if data["model_id"].nunique() != 1:
        raise DataValidationFaultyTag("PV forecast model id should be a constant value")

    if len(data.columns) < 4:
        raise DataValidationFaultyField("PV forecast data has to have at least 1 field columns")


def validate_pv_eval(data: pd.DataFrame):
    """
    This is the validation method for PV evaluation data which should follow the following
    structure:
    TAGS:
    - run: ZULU time of the model run in YYYYMMDDHH format
    - model_id : ID of the model
    :param data: dataframe to check
    raises DataValidationError if data is faulty
    """
    basic_validation(data)
    run_tag_validation(data)

    if "model_id" not in data.columns:
        raise DataValidationFaultyTag("PV forecast data has no model id tag column")

    if data["model_id"].nunique() != 1:
        raise DataValidationFaultyTag("PV forecast model id should be a constant value")

    if len(data.columns) < 4:
        raise DataValidationFaultyField("PV forecast data has to have at least 1 field columns")


def convert_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    This method ensures that the data types of the columns are consistent
    :param data: dataframe to convert
    raises if data cant be converted
    """
    strings = ["model"]
    ints = ["run", "loader_id", "cell_id", "model_id"]

    # There is a problem with tz handling in test, for this and to avoid unwanted behavior the tz is
    # forced to be a datetime.timezone object
    if not isinstance(df.index, pd.DatetimeIndex):
        # Convert index to DatetimeIndex
        df.index = pd.to_datetime(df.index, utc=True)

    if df.index.tz is None:
        # Ensure that the index is timezone aware

        df.index = df.index.tz_localize("UTC")

    if not isinstance(df.index.tz, datetime.timezone):
        df.index = df.index.tz_convert(None)
        df.index = df.index.tz_localize("UTC")

    for col in df.columns:
        # If the column is not in the exceptions list, try to convert it to a numeric type
        if col in strings:
            df[col] = df[col].astype("string")
            continue

        if col in ints:
            df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
            continue

        df[col] = pd.to_numeric(df[col], errors='raise', downcast='float')
    return df

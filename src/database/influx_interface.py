"""
This file contains the interface to the influx database. It wraps the influxdb_client and provides
methods to write and read data from the database in a structured way.
"""

import os
from datetime import datetime

import pandas as pd
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

from src.database.data_classes import Measurements
from src.database.data_validation import validate_pv_data, validate_pv_forecast, \
    validate_weather_forecast, validate_pv_eval, convert_data_types
from src.utils.dwd_tools import get_timestamp_from_runid
from src.utils.logging import get_default_logger

# to prevent bugs every query is limited to 3 years in the past.
MIN_QR = "-3y"
MAX_QR = "11d"
# keeping track of the api version
API_VERSION = "v1.0"
APIV_FILTER = f'|> filter(fn: (r) => r.api_version == "{API_VERSION}")'

logger = get_default_logger(__name__)


class InfluxInterface:
    """
    Class for interfacing with the internal influx database
    """

    def __init__(self, url: str, token: str, org: str, bucket: str):
        """
        Interface to the influx database
        :param url: url to database
        :param token: access token
        :param org: organization to use for access
        :param bucket: bucket for read and write
        """
        self.url = url
        self.token = token
        self.client = InfluxDBClient(url=url, token=token, org=org, timeout=100000)
        self.read_api = self.client.query_api()
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.delete_api = self.client.delete_api()
        self.org = org
        self.bucket = bucket

    @classmethod
    def from_env(cls):
        """
        Creates an InfluxInterface object from environment variables
        :return: InfluxInterface object
        """

        host_name = os.environ.get("DOCKER_INFLUXDB_INIT_HOST")
        port = os.environ.get("DOCKER_INFLUXDB_INIT_PORT")
        url = f"http://{host_name}:{port}"
        return cls(
            url=url,
            token=os.environ.get("DOCKER_INFLUXDB_INIT_ADMIN_TOKEN"),
            org=os.environ.get("DOCKER_INFLUXDB_INIT_ORG"),
            bucket=os.environ.get("DOCKER_INFLUXDB_INIT_BUCKET")
        )

    def __del__(self):
        self.client.close()

    def health(self) -> bool:
        """
        Returns the health of the database
        :return: True if healthy, False if not
        """
        return self.client.ping()

    ################################################################################################
    # WRITE METHODS #
    ################################################################################################
    def write_weather_forecast(self, df: pd.DataFrame, model: str, run: int, loader_id: int,
                               cell_id: int):
        """
        Writes a dataframe of a weather forecast to the influx database.
        :param model: Name of the model used
        :param run: ZULU time of the model run in YYYYMMDDHH format
        :param loader_id: id of the loader, which created the data
        :param cell_id: id of the cell, which the forecast is for
        :param df: Dataframe to write
        """
        df = df.copy()

        df["model"] = model
        df["run"] = run
        df["loader_id"] = loader_id
        df["cell_id"] = cell_id
        df["api_version"] = API_VERSION

        validate_weather_forecast(df)

        self.write_api.write(bucket=self.bucket,
                             org=self.org,
                             record=df,
                             data_frame_measurement_name=Measurements.WEATHER_FORECAST,
                             data_frame_tag_columns=["model", "run", "loader_id", "cell_id",
                                                     "api_version"])

    def write_pv_forecast(self, df: pd.DataFrame, model_id: int, run: int):
        """
        Writes a dataframe of a pv forecast to the influx database.
        :param model_id: id of the model used
        :param run: ZULU time of the model run in YYYYMMDDHH format
        :param df: Dataframe to write
        """
        df = df.copy()
        df["run"] = run
        df["model_id"] = model_id
        df["api_version"] = API_VERSION

        validate_pv_forecast(df)

        self.write_api.write(bucket=self.bucket,
                             org=self.org,
                             record=df,
                             data_frame_measurement_name=Measurements.PV_FORECAST,
                             data_frame_tag_columns=["run", "model_id", "api_version"])

    def write_pv_data(self, df: pd.DataFrame, loader_id: int):
        """
        Writes a dataframe to the influx database, with the run_id as tag
        :param loader_id: id of the loader, which created the data
        :param df: Dataframe to write
        """
        df = df.copy()
        df["loader_id"] = loader_id
        df["api_version"] = API_VERSION

        validate_pv_data(df)

        self.write_api.write(bucket=self.bucket,
                             org=self.org,
                             record=df,
                             data_frame_measurement_name=Measurements.PV_MEASUREMENT,
                             data_frame_tag_columns=["loader_id", "api_version"])

    def write_eval_data(self, df: pd.DataFrame, run: int, model_id: int):
        """
        Writes a dataframe of a pv evaluation (a numerical measure to compare prediction and actual
        output) to the influx database.
        :param df: data to write
        :param run: ZULU time of the model run the eval is for in YYYYMMDDHH format
        :param model_id: model id of the model used
        """
        df = df.copy()
        df["run"] = run
        df["model_id"] = model_id
        df["api_version"] = API_VERSION

        validate_pv_eval(df)

        self.write_api.write(bucket=self.bucket,
                             org=self.org,
                             record=df,
                             data_frame_measurement_name=Measurements.PV_EVALUATION,
                             data_frame_tag_columns=["model_id", "run", "api_version"])

    ################################################################################################
    # Read METHODS #
    ################################################################################################

    def get_last_entry_of_pv_measurement(self, field: str) -> (float | None, datetime | None):
        """
        Returns the last value of a field in a measurement
        :param measurement: measurement name
        :param field: field name
        :return: tuple of (value, timestamp), if not found None
        """

        query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: {MIN_QR}, stop: {MAX_QR})
                {APIV_FILTER}
                |> filter(fn: (r) => r._measurement == "{Measurements.PV_MEASUREMENT}")
                |> filter(fn: (r) =>  r._field == "{field}")
                |> group()
                |> sort(columns: ["_time"])'''
        result = self.read_api.query(query, org=self.org)
        try:
            last_record = result[0].records[-1]
            value = last_record.values["_value"]
            ts = last_record.values["_time"]
            return value, ts
        except IndexError:
            return None, None

    def get_existing_run_tags(self, measurement: str, component_id: int = None) -> [int]:
        """
        Returns a list of all run tags, for a given measurement, can be weather_forecast or
        pv_forecast
        :param measurement: measurement to get the run tags from
        :param component_id: id of the component associated with the run
        :return: list of run ids format: YYYYMMDDHH e.g. 2023071815
        """
        if measurement not in Measurements.ALL:
            raise ValueError(f"Measurement must be one of {Measurements.ALL}")
        if measurement == Measurements.PV_MEASUREMENT:
            raise ValueError(f"Measurement {Measurements.PV_MEASUREMENT} does not have run tags")

        discriminator = ""
        if measurement == Measurements.WEATHER_FORECAST:
            discriminator = f'|> filter(fn: (r) => r.loader_id == "{component_id}")'
        elif measurement == Measurements.PV_FORECAST:
            discriminator = f'|> filter(fn: (r) => r.model_id == "{component_id}")'

        query = f'''
            from(bucket: "{self.bucket}")
                |> range(start: {MIN_QR},stop:{MAX_QR})
                |> filter(fn: (r) => r._measurement == "{measurement}")
                {discriminator}
                {APIV_FILTER}
                |> distinct(column: "run")
        '''

        result = self.read_api.query(query, org=self.org)

        result_list = []
        for table in result:
            for record in table.records:
                result_list.append(int(record.values.get("_value")))

        # remove duplicates and sort (could be optimized with a set in the future)
        return sorted(list(set(result_list)))

    def get_missing_forecast_ids(self, loader_id: int, model_id: int) -> [int]:
        """
        Returns a list of all run ids for which no pv forecast exists
        :param loader_id: id of the loader
        :param model_id: id of the model
        :return: list of run ids format: YYYYMMDDHH e.g. 2023071815
        """
        forecast_runs = self.get_existing_run_tags(measurement=Measurements.WEATHER_FORECAST,
                                                   component_id=loader_id)
        pv_runs = self.get_existing_run_tags(measurement=Measurements.PV_FORECAST,
                                             component_id=model_id)

        missing_runs = [x for x in forecast_runs if x not in pv_runs]

        return missing_runs

    def get_missing_evaluation_ids(self, model_id: int) -> [int]:
        """
        Returns a list of all run ids for which no pv evaluation exists
        :param model_id: id of the model
        :return: list of run ids format: YYYYMMDDHH e.g. 2023071815
        """
        pv_runs = self.get_existing_run_tags(measurement=Measurements.PV_FORECAST,
                                             component_id=model_id)
        eval_runs = self.get_existing_run_tags(measurement=Measurements.PV_EVALUATION)

        missing_runs = [x for x in pv_runs if x not in eval_runs]

        return missing_runs

    def get_pv_data(self, start_time: datetime = None, stop_time: datetime = None,
                    targets: [str] = None, loader_id: int = None,
                    keep_metadata: bool = False) -> pd.DataFrame:
        """
        Gets the hourly pv data for a given time range from the database, all fields in the
        pv_measurement table are summed up
        :param targets: list of fields to retrive, if sum of all is wanted, leave empty
        :param start_time: Start of the time range
        :param stop_time: End of the time range
        :param loader_id: id of the loader to get the data from
        :param keep_metadata: if true, metadata columns are kept
        :return: Dataframe with one column "Target" containing the summed up pv data
        """

        # set time range to default values if not given +1 for inclusiveness
        u_start_time = int(start_time.timestamp()) if start_time is not None else MIN_QR
        u_stop_time = int(stop_time.timestamp()) + 1 if stop_time is not None else MAX_QR

        # create a filter for the targets if given to only get the wanted fields

        loader_discriminator = ""
        if loader_id:
            loader_discriminator = f'|> filter(fn: (r) => r.loader_id == "{loader_id}")'

        target_discriminator = ""
        if targets:
            target_discriminator = ('|> filter(fn: (r) => r._field == "'
                                    + '" or r._field == "'.join(targets) + '")')

        query = f'''from(bucket: "pv-data")
            |> range(start: {u_start_time}, stop: {u_stop_time})
            |> filter(fn: (r) => r._measurement == "{Measurements.PV_MEASUREMENT}")
            {APIV_FILTER}
            {target_discriminator}
            {loader_discriminator}
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''

        result = self.read_api.query_data_frame(query, data_frame_index=["_time"], org=self.org)

        # with csv import multiple values for the same target can be given back. They are removed
        # here

        if isinstance(result, list):
            raise ValueError("ambiguous result, multiple tables returned")

        if result.empty:
            raise ValueError("No pv_data found for given parameters: "
                             "start_time: {start_time}, stop_time: {stop_time}, targets: {targets}")

        result = result[~result.index.duplicated()]

        # discard metadata
        result.drop(columns=["result", "table", "_start", "_stop", "_measurement",
                             "api_version"], inplace=True)
        if not keep_metadata:
            result.drop(columns=["loader_id"], inplace=True)

        return convert_data_types(result)

    def get_weather_forecasts(self, loader_id: int = None, cell_id: int = None,
                              run: int = None, keep_metadata: bool = False) -> pd.DataFrame:
        """
        Returns a dataframe with all weather forecasts from a loader or a specific run when given a
        run id. It is possible to filter for a specific cell_id. Either loader_id or cell_id has to
        be given.
        :param loader_id: id of the loader to get the data from
        :param cell_id: id of the cell to get the data from
        :param run: run id
        :param keep_metadata: if true, metadata columns are kept
        :return: dataframe with the weather forecast(s)
        """

        if loader_id is None and cell_id is None:
            raise ValueError("Either loader_id or cell_id has to be given")
        if loader_id is not None and cell_id is not None:
            raise ValueError("Either loader_id or cell_id has to be given, not both")

        run_discriminator = ""
        if run is not None:
            run_discriminator = f'|> filter(fn: (r) => r.run == "{run}")'

        cell_discriminator = ""
        if cell_id is not None:
            cell_discriminator += f'|> filter(fn: (r) => r.cell_id == "{cell_id}")'

        loader_discriminator = ""
        if loader_id is not None:
            loader_discriminator = f'|> filter(fn: (r) => r.loader_id == "{loader_id}")'

        query = f'''
        from(bucket: "{self.bucket}")
            |> range(start: {MIN_QR}, stop: {MAX_QR})
            |> filter(fn: (r) => r._measurement == "{Measurements.WEATHER_FORECAST}")
            {APIV_FILTER}
            {run_discriminator}
            {cell_discriminator}
            {loader_discriminator}
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''
        result = self.read_api.query_data_frame(query, data_frame_index=["_time"], org=self.org)

        if result.empty:
            raise ValueError(f"No weather forecast data found for given parameters:"
                             f"loader_id: {loader_id}, cell_id: {cell_id}, run: {run}")

        result.drop(columns=["result", "table", "_start", "_stop", "_measurement", "api_version"],
                    inplace=True)

        if not keep_metadata:
            result.drop(columns=["loader_id", "cell_id"], inplace=True)
            if run is not None:
                result.drop(columns=["run"], inplace=True)

        return convert_data_types(result)

    def get_training_examples(self, cell_id: int, target: str,
                              keep_metadata: bool = False) -> pd.DataFrame:
        """
        This query returns a dataframe with all available training examples, consisting of forecast
        data and matching pv data. For every timestep, latest forecast run is used insuring that the
        data is as accurate as possible.
        The result is a single continuous dataframe, with training data
        :param cell_id: id of the forecast cell to get the input data from
        :param target: target field to retriv
        :param keep_metadata: if true, metadata columns are kept
        :return: Dataframe with all training examples and time index.
        """
        result = self.get_weather_forecasts(cell_id=cell_id, keep_metadata=True)

        # only keep the latest forecast for every time step. This operation is very slow, might have
        # to optimize it in the future
        rows = []
        for _, df in result.groupby(result.index):
            row = df.sort_values("run", ascending=False).head(1)
            if get_timestamp_from_runid(int(row["run"].values[0])) + pd.Timedelta(days=1) > \
                    row.index[0]:
                rows.append(row)

        forecast_data = pd.concat(rows)
        forecast_data.sort_index(inplace=True)

        pv_data = self.get_pv_data(forecast_data.index.min(), forecast_data.index.max(), [target],
                                   keep_metadata=True)

        pv_data.rename(columns={"loader_id": "target_loader_id"}, inplace=True)
        forecast_data.rename(columns={"loader_id": "weather_loader_id"}, inplace=True)

        forecast_data = forecast_data.join(pv_data, how='inner')

        forecast_data.sort_index(inplace=True)

        if not keep_metadata:
            forecast_data.drop(
                columns=["target_loader_id", "weather_loader_id", "cell_id", "run", "model"],
                inplace=True)

        if any(forecast_data.index.duplicated()):
            raise ValueError("Duplicated timestamps in the training data")

        return forecast_data

    def get_forecast(self, target: str, run_id: str = None) -> (pd.DataFrame, str):
        """
        Retrieves the forecast for a given target. If no run_id is provided, it fetches the forecast
        for the highest run tag available for the target.

        :param target: The target field to retrieve the forecast for.
        :param run_id: The run_id for which to retrieve the forecast. If None, the highest available
        run_id is used.
        :return: A tuple containing a DataFrame with the forecast and the run_id used for the
        forecast.
        """

        if run_id is None:
            run_id = self.get_max_run(target)

        ts_stop = get_timestamp_from_runid(run_id) + pd.Timedelta(days=14)
        ts_start = get_timestamp_from_runid(run_id) - pd.Timedelta(days=1)

        ts_stop = int(ts_stop.timestamp())
        ts_start = int(ts_start.timestamp())

        query = f'''
        from(bucket: "pv-data")
            |> range(start: {ts_start},stop: {ts_stop})
            {APIV_FILTER}
            |> filter(fn: (r) => r["_measurement"] == "{Measurements.PV_FORECAST}")
            |> filter(fn: (r) => r["run"] == "{run_id}")
            |> filter(fn: (r) => r["_field"] == "{target}")
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''

        result = self.read_api.query_data_frame(query, org=self.org, data_frame_index=["_time"])

        if result.empty:
            raise ValueError(f"No data found for target: {target}")

        result = result[[target]]

        return result, run_id

    def get_max_run(self, target: str) -> str:
        """
        Returns the highest run tag for a given target field.

        :param target: The target field to retrieve the maximum run tag for.
        :return: The maximum run tag as a string.
        """

        query = f'''
        from(bucket: "pv-data")
            |> range(start: {MIN_QR}, stop: {MAX_QR})
            {APIV_FILTER}
            |> filter(fn: (r) => r._measurement == "{Measurements.PV_FORECAST}")
            |> filter(fn: (r) => r._field == "{target}") ''' + '''
            |> map(fn: (r) => ({r with _value: int(v: r.run)}))
            |> group()
            |> max()
            |> group(columns: ["_value"], mode: "by")
            |> top(n: 1)
        '''
        result = self.read_api.query(query, org=self.org)

        tables = list(result)
        if len(tables) != 1:
            raise ValueError(f"Ambiguous result for target: {target}, this may be due to false data"
                             f" in the database.")

        records = tables[0].records
        if len(records) == 0:
            raise ValueError(f"No run tag found for target: {target}")

        test = str(records[0].get_value())

        return test

    ################################################################################################
    # Delete METHODS #
    ################################################################################################

    def delete_measures(self, measurement: str):
        """
        Deletes al data from the database, as well as model accuracy data
        :param measurement:
        :return: True if successful, False if not
        """
        if measurement not in Measurements.ALL:
            raise ValueError(f"Cant Delete measure:{measurement} since it is not a "
                             f"correct measurement")

        self.delete_api.delete(start="1970-01-01T00:00:00Z", stop="2100-01-01T00:00:00Z",
                               predicate=f'_measurement="{measurement}"',
                               bucket=self.bucket, org=self.org)


influx_interface = InfluxInterface.from_env()

if __name__ == '__main__':
    influx_interface.get_forecast("power")

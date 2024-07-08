import os
import time
import unittest
from secrets import token_urlsafe

import numpy as np
import pandas as pd
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from src.database.data_classes import Measurements
from src.database.data_validation import convert_data_types
from src.database.influx_interface import InfluxInterface
from src.utils.static import test_data_root_path, test_weather_backup_file, test_target_backup_file

influx_envs = {
    "DOCKER_INFLUXDB_INIT_MODE": "setup",
    "DOCKER_INFLUXDB_INIT_USERNAME": "admin",
    "DOCKER_INFLUXDB_INIT_PASSWORD": "password",
    "DOCKER_INFLUXDB_INIT_ADMIN_TOKEN": token_urlsafe(32),
    "DOCKER_INFLUXDB_INIT_ORG": "pv-data",
    "DOCKER_INFLUXDB_INIT_BUCKET": "pv-data",
    "DOCKER_INFLUXDB_INIT_PORT": 8086,
    "DOCKER_INFLUXDB_INIT_HOST": "dind" if os.environ.get("CI") else "localhost",
}


def create_test_env():
    interface: InfluxInterface
    container = DockerContainer("influxdb:2.7.1")
    container.with_bind_ports(8086, 8086)
    for key, value in influx_envs.items():
        container.with_env(key, value)
        os.environ[key] = str(value)
    container.with_name("influxdb_test")
    container.start()
    wait_for_logs(container, "lvl=info msg=Listening")
    time.sleep(1)
    return container


class InfluxInterFaceTestBase(unittest.TestCase):

    def setUp(self):
        self.container = create_test_env()
        self.interface = InfluxInterface.from_env()
        self.valid_weather_data = []
        for x in os.listdir(os.path.join(test_data_root_path, "mosmix_csv")):
            if ".csv" in x:
                self.valid_weather_data.append(
                    pd.read_csv(os.path.join(test_data_root_path, "mosmix_csv", x), index_col=0,
                                parse_dates=True).convert_dtypes())
        self.valid_pv_data = pd.DataFrame({'value1': np.random.rand(10),
                                           'value2': np.random.rand(10)},
                                          index=pd.date_range(start="2023-01-01", periods=10,
                                                              freq="h", tz="UTC",
                                                              name="_time")).convert_dtypes()

        self.start = pd.Timestamp(year=2023, month=9, day=1, hour=1, tz="UTC")
        self.stop = pd.Timestamp(year=2023, month=11, day=1, hour=1, tz="UTC")

        self.pv_data_backup = pd.read_csv(test_target_backup_file, index_col="_time",
                                          parse_dates=True)[self.start:self.stop]

        self.weather_data_backup = pd.read_csv(test_weather_backup_file, index_col="_time",
                                               parse_dates=True)

        self.weather_data_backup = self.weather_data_backup[
            self.weather_data_backup.index < self.stop]
        self.weather_data_backup = self.weather_data_backup[
            self.weather_data_backup.index > self.start]

        self.pv_data_backup = convert_data_types(self.pv_data_backup)
        self.weather_data_backup = convert_data_types(self.weather_data_backup)



    def tearDown(self):
        self.container.stop()
        time.sleep(1)


class InfluxInterfaceTest(InfluxInterFaceTestBase):

    def test_connection(self):
        """Test the connection to the InfluxDB database."""
        ping = self.interface.health()
        self.assertEqual(True, ping, "If this fails there might be a problem with the InfluxDB "
                                     "container.")

    def test_read_and_write_pv(self):
        loader_id = 42
        self.interface.write_pv_data(self.pv_data_backup, loader_id=loader_id)

        correct_read = self.interface.get_pv_data(targets=["power"], loader_id=loader_id)

        pd.testing.assert_frame_equal(self.pv_data_backup, correct_read)

        correct_read_no_discrimination = self.interface.get_pv_data()

        pd.testing.assert_frame_equal(self.pv_data_backup, correct_read_no_discrimination)

        self.interface.write_pv_data(self.valid_pv_data, loader_id=33333)

        correct_read_only_field_name = self.interface.get_pv_data(targets=["power"])

        pd.testing.assert_frame_equal(self.pv_data_backup, correct_read_only_field_name)

        with self.assertRaises(ValueError):
            self.interface.get_pv_data()

        with self.assertRaises(ValueError):
            self.interface.get_pv_data(targets=["wrong_target"])

        start = pd.Timestamp(year=2023, month=10, day=1, hour=5, tz="UTC")
        stop = pd.Timestamp(year=2023, month=10, day=12, hour=3, tz="UTC")
        correct_read_with_timerange = self.interface.get_pv_data(targets=["power"], stop_time=stop,
                                                                 start_time=start)

        self.assertEqual(correct_read_with_timerange.index.min(), start)
        self.assertEqual(correct_read_with_timerange.index.max(), stop)

    def test_read_and_write_weather_forecast(self):
        cell_id = self.weather_data_backup["cell_id"].values[0]
        model = self.weather_data_backup["model"].values[0]
        run_sample = self.weather_data_backup["run"].values[len(self.weather_data_backup)//2]
        loader_id = 42

        write_df = self.weather_data_backup.drop(columns=["cell_id", "model"])

        for run, df in write_df.groupby("run"):
            self.interface.write_weather_forecast(df, model=model, run=run, loader_id=loader_id,
                                                  cell_id=cell_id)

        retrieve_data = self.interface.get_weather_forecasts(cell_id=cell_id,keep_metadata=True)

        self.assertEqual(retrieve_data["loader_id"].values[0], loader_id)

        pd.testing.assert_frame_equal(self.weather_data_backup,
                                      retrieve_data[self.weather_data_backup.columns])

        with self.assertRaises(ValueError):
            self.interface.get_weather_forecasts()

        with self.assertRaises(ValueError):
            self.interface.get_weather_forecasts(cell_id=3333, loader_id=2222)

        single_run = self.interface.get_weather_forecasts(loader_id=loader_id, run=run_sample,keep_metadata=True)

        for i,row in single_run.iterrows():
           self.assertEqual(row["run"],run_sample)

        self.assertEqual(len(single_run), 247)

    def test_run_tags(self):
        cell_id = self.weather_data_backup["cell_id"].values[0]
        model = self.weather_data_backup["model"].values[0]

        w_loader_id = 42
        t_loader_id = 24
        model_id = 12
        write_df = self.weather_data_backup.drop(columns=["cell_id", "model"])

        # delete incomplete runs
        grouped_df = [(k, df) for (k, df) in write_df.groupby("run") if len(df) == 247]
        runs = [k for k, df in grouped_df]
        # write weather forecast
        for run, df in grouped_df:
            self.interface.write_weather_forecast(df, model=model, run=run, loader_id=w_loader_id,
                                                  cell_id=cell_id)

        # write pv data
        self.interface.write_pv_data(self.pv_data_backup, loader_id=t_loader_id)

        # all runs forecast should be missing
        retrive = self.interface.get_missing_forecast_ids(loader_id=w_loader_id, model_id=model_id)
        self.assertListEqual(runs, retrive)

        # add mock pv forecast
        for run, df in grouped_df:
            pv_forecast_mock = df[["Rad1h"]].copy().rename(columns={"Rad1h": "power"})
            self.interface.write_pv_forecast(pv_forecast_mock,
                                             model_id=model_id, run=run)

        # no forecast runs should be missing
        retrive = self.interface.get_missing_forecast_ids(loader_id=w_loader_id, model_id=model_id)
        self.assertListEqual([], retrive)

        # all eval runs should be missing
        retrive = self.interface.get_missing_evaluation_ids(model_id=model_id)
        self.assertListEqual(runs, retrive)

    def test_training_data(self):
        cell_id = self.weather_data_backup["cell_id"].values[0]
        model = self.weather_data_backup["model"].values[0]
        w_loader_id = 12
        t_loader_id = 10

        write_df = self.weather_data_backup.drop(columns=["cell_id", "model"])
        grouped_df = [(k, df) for (k, df) in write_df.groupby("run") if len(df) == 247]

        for run, df in grouped_df:
            self.interface.write_weather_forecast(df, model=model, run=run, loader_id=w_loader_id,
                                                  cell_id=cell_id)

        self.interface.write_pv_data(self.pv_data_backup, loader_id=t_loader_id)

        train_data = self.interface.get_training_examples(target="power", cell_id=cell_id,
                                                          keep_metadata=True)

        self.assertEqual(train_data["target_loader_id"].iloc[0], t_loader_id)
        self.assertEqual(train_data["weather_loader_id"].iloc[0], w_loader_id)
        self.assertTrue("power" in train_data.columns)

        [self.assertLessEqual(len(df), 24) for (k,df) in train_data.groupby("run")]

        correlation = train_data[["power", "Rad1h"]].corr()

        self.assertGreater(correlation["power"]["Rad1h"], 0.7)

    def test_data_deletion(self):
        cell_id = self.weather_data_backup["cell_id"].values[0]
        model = self.weather_data_backup["model"].values[0]
        w_loader_id = 12
        t_loader_id = 10
        model_id = 24

        write_df = self.weather_data_backup.drop(columns=["cell_id", "model"])
        grouped_df = [(k, df) for (k, df) in write_df.groupby("run") if len(df) == 247]

        for run, df in grouped_df:
            self.interface.write_weather_forecast(df, model=model, run=run, loader_id=w_loader_id,
                                                  cell_id=cell_id)
        self.interface.write_pv_data(self.pv_data_backup, loader_id=t_loader_id)

        self.interface.get_pv_data(loader_id=t_loader_id)
        self.interface.get_weather_forecasts(loader_id=w_loader_id)

        self.interface.delete_measures(Measurements.PV_MEASUREMENT)

        with self.assertRaises(ValueError):
            self.interface.get_pv_data(loader_id=t_loader_id)

        self.interface.delete_measures(Measurements.WEATHER_FORECAST)

        with self.assertRaises(ValueError):
            self.interface.get_weather_forecasts(loader_id=w_loader_id)

    def test_get_last_field(self):
        t_loader_id = 10
        self.interface.write_pv_data(self.pv_data_backup, loader_id=t_loader_id)
        last_index = self.pv_data_backup.index.max()
        last_value = self.pv_data_backup.iloc[-1]["power"]

        r_value, r_index = self.interface.get_last_entry_of_pv_measurement("power")

        self.assertEqual(r_value, last_value)
        self.assertEqual(r_index, last_index)

    def test_pv_forecast(self):
        model_id = 24

        write_df = self.weather_data_backup.drop(columns=["cell_id", "model"])
        grouped_df = [(k, df) for (k, df) in write_df.groupby("run") if len(df) == 247]

        max_run_id = str(max([int(run) for run, df in grouped_df]))

        # add mock pv forecast
        for run, df in grouped_df:
            pv_forecast_mock = df[["Rad1h"]].copy().rename(columns={"Rad1h": "power"})
            self.interface.write_pv_forecast(pv_forecast_mock,
                                             model_id=model_id, run=run)

        run, run_id = self.interface.get_forecast(target="power")

        self.assertEqual(run_id, max_run_id, "Run ID does not match function did not return newest run")

        self.assertEqual(len(run),247, "Returned forecast does not have the correct length")




if __name__ == '__main__':
    unittest.main()

import unittest

import pandas as pd

from src.database.data_validation import validate_pv_data, DataValidationError, \
    validate_weather_forecast, DataValidationFaultyTag, DataValidationFaultyField, \
    DataValidationFaultyIndex, validate_pv_forecast, validate_pv_eval


class TestValidatePVData(unittest.TestCase):
    def setUp(self):
        # Setting up valid PV data as a common fixture for tests
        datetime_index = pd.date_range(start="2023-01-01", end="2023-01-02", freq="h")
        self.valid_pv_data = pd.DataFrame(index=datetime_index,
                                          data={'loader_id': 'loader_1', 'pv_output':
                                              [1.0] * len(datetime_index),
                                                'api_version': '1.0'
                                                })


    def test_validate_pv_data_valid(self):
        # Test with valid data structure; expecting no exception to be raised
        try:
            validate_pv_data(self.valid_pv_data)
        except DataValidationError:
            self.fail("validate_pv_data raised DataValidationError unexpectedly!")

    def test_validate_pv_data_without_loader_id(self):
        # Removing 'loader_id' column to simulate invalid data
        data_without_loader_id = self.valid_pv_data.drop(columns=['loader_id'])
        with self.assertRaises(DataValidationFaultyTag):
            validate_pv_data(data_without_loader_id)

    def test_validate_pv_data_non_hourly_index(self):
        # Creating non-hourly datetime index to simulate invalid data
        datetime_index = pd.date_range(start="2023-01-01", end="2023-01-02", freq="30min")
        data_non_hourly_index = pd.DataFrame(index=datetime_index,
                                             data={'loader_id': 'loader_1', 'pv_output':
                                                 [1.0] * len(datetime_index),
                                                   'api_version': '1.0'})
        with self.assertRaises(DataValidationFaultyIndex):
            validate_pv_data(data_non_hourly_index)

    def test_validate_pv_data_no_field_columns(self):
        # Keeping only 'loader_id' column to simulate data without field columns
        data_no_field_columns = self.valid_pv_data.drop(columns=['pv_output'])
        with self.assertRaises(DataValidationFaultyField):
            validate_pv_data(data_no_field_columns)


class TestValidateWeatherForecast(unittest.TestCase):
    def setUp(self):
        # Creating a valid DataFrame
        datetime_index = pd.date_range(start="2023-01-01", periods=24, freq='h')
        self.valid_data = pd.DataFrame(
            {
                'model': ['mosmix'] * 24,
                'run': ['2023010100'] * 24,
                'loader_id': ['loader_1'] * 24,
                'cell_id': ['cell_1'] * 24,
                'temperature': range(24),
                'humidity': range(24),
                'api_version': ['1.0'] * 24
            },
            index=datetime_index
        )

    def test_validate_weather_forecast_valid(self):
        # Testing with valid data structure
        try:
            validate_weather_forecast(self.valid_data)
        except DataValidationError:
            self.fail("validate_weather_forecast raised DataValidationError unexpectedly!")

    def test_weather_forecast_without_model_tag(self):
        # Remove 'model' tag to simulate invalid data
        data_without_model = self.valid_data.drop(columns=['model'])
        with self.assertRaises(DataValidationFaultyTag):
            validate_weather_forecast(data_without_model)

        data_with_wrong_model = self.valid_data.copy()
        data_with_wrong_model['model'] = 'wrong_model'

        with self.assertRaises(DataValidationFaultyTag):
            validate_weather_forecast(data_with_wrong_model)

    def test_weather_forecast_non_hourly_index(self):
        # Creating non-hourly datetime index to simulate invalid data
        datetime_index = pd.date_range(start="2023-01-01", periods=24, freq='30min')
        data_non_hourly_index = self.valid_data.copy()
        data_non_hourly_index.index = datetime_index
        with self.assertRaises(DataValidationFaultyIndex):
            validate_weather_forecast(data_non_hourly_index)

    def test_weather_forecast_missing_weather_parameter(self):
        # Remove a weather parameter column to simulate invalid data
        data_missing_parameter = self.valid_data.drop(columns=['temperature', 'humidity'])
        with self.assertRaises(DataValidationError):
            validate_weather_forecast(data_missing_parameter)


class TestValidatePVForecast(unittest.TestCase):
    def setUp(self):
        # Creating a valid DataFrame
        datetime_index = pd.date_range(start="2023-01-01", periods=24, freq='h')
        self.valid_data = pd.DataFrame(
            {
                'model_id': ['model_id'] * 24,
                'run': ['2023010100'] * 24,
                'target': range(24),
                'api_version': '1.0'
            },
            index=datetime_index
        )

    def test_pv_forecast_valid(self):
        # Testing with valid data structure
        try:
            validate_pv_forecast(self.valid_data)
        except DataValidationError:
            self.fail("validate_weather_forecast raised DataValidationError unexpectedly!")

    def test_pv_forecast_non_hourly_index(self):
        # Creating non-hourly datetime index to simulate invalid data
        datetime_index = pd.date_range(start="2023-01-01", periods=24, freq='30min')
        data_non_hourly_index = self.valid_data.copy()
        data_non_hourly_index.index = datetime_index
        with self.assertRaises(DataValidationFaultyIndex):
            validate_pv_forecast(data_non_hourly_index)

    def test_pv_forecast_missing_target(self):
        # Remove a weather parameter column to simulate invalid data
        data_missing_parameter = self.valid_data.drop(columns=['target'])
        with self.assertRaises(DataValidationFaultyField):
            validate_pv_forecast(data_missing_parameter)


class TestValidatePVEval(unittest.TestCase):
    def setUp(self):
        # Creating a valid DataFrame
        datetime_index = pd.date_range(start="2023-01-01", periods=24, freq='h')
        self.valid_data = pd.DataFrame(
            {
                'model_id': ['model_id'] * 24,
                'run': ['2023010100'] * 24,
                'eval_metric': range(24),
                'api_version': '1.0'
            },
            index=datetime_index
        )

    def test_pv_eval_valid(self):
        # Testing with valid data structure
        try:
            validate_pv_eval(self.valid_data)
        except DataValidationError:
            self.fail("validate_weather_forecast raised DataValidationError unexpectedly!")

    def test_pv_eval_non_hourly_index(self):
        # Creating non-hourly datetime index to simulate invalid data
        datetime_index = pd.date_range(start="2023-01-01", periods=24, freq='30min')
        data_non_hourly_index = self.valid_data.copy()
        data_non_hourly_index.index = datetime_index
        try:
            validate_pv_eval(data_non_hourly_index)
        except DataValidationError:
            self.fail("validate_weather_forecast raised DataValidationError unexpectedly!")

    def test_pv_eval_missing_target(self):
        # Remove a weather parameter column to simulate invalid data
        data_missing_parameter = self.valid_data.drop(columns=['eval_metric'])
        with self.assertRaises(DataValidationFaultyField):
            validate_pv_eval(data_missing_parameter)

    def test_pv_eval_missing_model_id(self):
        # Remove a weather parameter column to simulate invalid data
        data_missing_parameter = self.valid_data.drop(columns=['model_id'])
        with self.assertRaises(DataValidationFaultyTag):
            validate_pv_eval(data_missing_parameter)



if __name__ == '__main__':
    unittest.main()

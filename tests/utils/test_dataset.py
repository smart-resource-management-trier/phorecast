import random
import unittest

import numpy as np
import pandas as pd
from hypothesis import given, strategies as st, settings

from src.utils.dataset import windowing, split_windows, get_dataset_from_windows, \
    attach_solar_positions, create_tf_dataset
from src.utils.static import test_dataset_file

dataset = pd.read_csv(test_dataset_file, index_col="TIMESTAMP", parse_dates=True)[0:1000]


class TestDataset(unittest.TestCase):
    @settings(deadline=None)
    @given(window_size=st.integers(min_value=6, max_value=48),
           stride=st.integers(min_value=6, max_value=20),
           max_missing=st.integers(min_value=1, max_value=6))
    def test_windowing(self, window_size, stride, max_missing):
        test_dataset = dataset.copy()

        deletion_index_list = []
        max_index = len(test_dataset) - 1
        for x in range(5):
            del_size = 50
            index_n_del = random.randint(0, max_index - del_size)
            index_del = test_dataset.index[index_n_del:index_n_del + del_size]
            deletion_index_list.extend(index_del)

        for x in range(20):
            index_del = dataset.index[random.randint(0, len(test_dataset) - 1)]
            deletion_index_list.append(index_del)

        deletion_index_list = list(set(deletion_index_list))

        test_dataset.drop(axis="index", index=deletion_index_list, inplace=True)

        windows = windowing(test_dataset, window_size=window_size, stride=stride,
                            max_missing=max_missing)

        for window in windows:
            self.assertEqual(len(window), window_size, "window size is not correct")

            zero_row_count = (window == 0).all(axis=1).sum()
            self.assertLessEqual(zero_row_count, max_missing, "max_missing is exceeded")

            if window_size != 1:
                self.assertIsNotNone(pd.infer_freq(window.index), "window is not continuous")

            self.assertTrue(window.index.is_monotonic_increasing or
                            window.index.is_monotonic_decreasing, "window is not monotonic")

            self.assertTrue(window.index.is_unique, "window has duplicate indices")

    @settings(deadline=None)
    @given(test_ratio=st.floats(min_value=.1, max_value=.9),
           factor=st.integers(min_value=7, max_value=30))
    def test_split_windows(self, test_ratio, factor):
        weeks_in_test = 1
        windows = windowing(dataset.copy(), window_size=24, stride=6, max_missing=3)
        train, test = split_windows(windows, test_ratio=test_ratio, weeks_in_test=weeks_in_test,
                                    factor=factor, distinct=True)

        self.assertAlmostEqual(len(test) / len(windows), test_ratio, 0, "test ratio is not correct")

        max_index = max([window.index.max() for window in windows])

        for train_window in train:
            self.assertLessEqual(train_window.index.max(),
                                 max_index - pd.Timedelta(weeks=weeks_in_test),
                                 f"the last {weeks_in_test} weeks are in the train set")

        train_indexes = set([x for window in train for x in window.index])
        test_indexes = set([x for window in test for x in window.index])

        common_indexes = test_indexes.intersection(train_indexes)

        self.assertEqual(len(common_indexes), 0, "train and test should be distinct")

    @settings(deadline=None)
    @given(window_size=st.integers(min_value=1, max_value=100),
           stride=st.integers(min_value=1, max_value=20),
           max_missing=st.integers(min_value=1, max_value=20))
    def test_get_dataset_from_windows(self, window_size, stride, max_missing):
        windows = windowing(dataset.copy(), window_size=window_size, stride=stride,
                            max_missing=max_missing)
        cols_length = len(windows[0].columns)
        train, test = split_windows(windows)
        (train_x, train_y), index = get_dataset_from_windows(train, target="")

        self.assertNotEqual(len(train_x), 0, "train_x has to be not empty")
        self.assertNotEqual(len(index), 0, "index has to be not empty")

        self.assertEqual(len(train_y), 0,
                         "train_y has to be empty if no target is present")

        self.assertEqual(len(train_x.shape), 3, "train_x has to be 3 dimensional")

        self.assertEqual(train_x.shape[1], window_size,
                         "window size is not matching the 2 dimension")
        self.assertEqual(train_x.shape[2], cols_length,
                         "number of columns does not match the 3 dimension")

        (train_x, train_y), index = get_dataset_from_windows(train, target="WR01")

        self.assertNotEqual(len(train_x), 0, "train_x has to be not empty")
        self.assertNotEqual(len(index), 0, "index has to be not empty")
        self.assertEqual(len(train_x), len(train_y),
                         "train_x and train_y have to be the same length")
        self.assertEqual(len(train_x.shape), 3, "train_x has to be 3 dimensional")

        for x in [train_x, train_y]:
            self.assertEqual(x.shape[1], window_size,
                             "window size is not matching the 2 dimension")
        self.assertEqual(train_x.shape[2], cols_length - 1,
                         "number of columns does not match the 3 dimension")
        self.assertEqual(train_y.shape[2], 1,
                         "third dimension of train_y has to be 1")

class TestAttachSolarPositions(unittest.TestCase):

    def setUp(self):
        """Setup a basic DataFrame with UTC timestamps to use across tests."""
        self.dates = pd.date_range('2022-01-01', periods=3, freq='h', tz='UTC')
        self.df = pd.DataFrame(index=self.dates, data={'data_column': [1, 2, 3]})
        self.latitude = 40.7128  # Example latitude, e.g., New York City
        self.longitude = -74.0060  # Example longitude

    def test_attach_solar_positions(self):
        """Test if solar positions are correctly attached."""
        modified_df = attach_solar_positions(self.df.copy(), self.latitude, self.longitude)
        # Check if the new columns are added
        self.assertIn('azimuth', modified_df.columns)
        self.assertIn('elevation', modified_df.columns)
        self.assertIn('zenith', modified_df.columns)
        # Check if the number of rows remains unchanged
        self.assertEqual(len(modified_df), len(self.df))

    def test_error_on_existing_solar_positions(self):
        """Test if a ValueError is raised when solar position columns already exist."""
        # Manually add solar position columns to simulate the error condition
        df_with_solar = self.df.copy()
        for col in ['azimuth', 'elevation', 'zenith']:
            df_with_solar[col] = 0
        # Attempt to attach solar positions again and expect a ValueError
        with self.assertRaises(ValueError):
            attach_solar_positions(df_with_solar, self.latitude, self.longitude)


class TestCreateTFDatasetMultipleInputs(unittest.TestCase):
    def setUp(self):
        # Setup numpy array for features and labels
        self.features = np.random.rand(100, 10)  # 100 samples, 10 features each
        self.labels = np.random.randint(0, 2, 100)  # 100 binary labels
        self.empty_labels = np.array([])  # Empty labels
        self.batch_size = 10

    def test_single_array_input(self):
        # Test input with only features
        dataset = create_tf_dataset(self.features, self.batch_size)
        for batch in dataset:
            self.assertEqual(batch.numpy().shape[1], 10)  # Check feature size

    def test_tuple_with_empty_labels(self):
        # Test input tuple with empty labels
        dataset = create_tf_dataset((self.features, self.empty_labels), self.batch_size)
        for batch in dataset:
            self.assertEqual(batch.numpy().shape[1], 10)  # Check feature size

    def test_tuple_with_labels(self):
        # Test input tuple with non-empty labels
        dataset = create_tf_dataset((self.features, self.labels), self.batch_size)
        for features, labels in dataset:
            self.assertEqual(features.numpy().shape[0], self.batch_size)
            self.assertEqual(labels.numpy().shape[0], self.batch_size)
if __name__ == '__main__':
    unittest.main()

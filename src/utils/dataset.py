"""
Utilities for Dataset operations, like windowing or splitting
"""
import random

import numpy as np
import pandas as pd
import pvlib
import tensorflow as tf

from src.utils.logging import get_default_logger

logger = get_default_logger(__name__)


def create_tf_dataset(data: tuple | np.ndarray, batch_size: int,
                      shuffle: bool = False) -> tf.data.Dataset:
    """
    Create a TensorFlow Dataset from a numpy array with specified batch size,
    including shuffling, prefetching, and caching for optimized performance.

    :param data: (np.ndarray or tuple): Single numpy array of features or tuple of numpy arrays
        (features, optional labels).
    :param batch_size: batch size for the dataset
    :return: TensorFlow Dataset
    """
    buffer_size = None
    if isinstance(data, tuple):
        features, labels = data
        # Check if labels are provided
        buffer_size = len(features)
        if labels.size > 0:
            dataset = tf.data.Dataset.from_tensor_slices((features, labels))
        else:
            dataset = tf.data.Dataset.from_tensor_slices(features)
    else:
        dataset = tf.data.Dataset.from_tensor_slices(data)
        buffer_size = len(data)

    # Shuffle the dataset with a buffer size equal to the number of elements
    if shuffle:
        dataset = dataset.shuffle(buffer_size=buffer_size, reshuffle_each_iteration=True)

    # Batch the dataset with the specified batch size
    dataset = dataset.batch(batch_size)

    # Apply prefetching for performance optimization
    dataset = dataset.prefetch(tf.data.experimental.AUTOTUNE)

    dataset = dataset.cache()
    return dataset


def attach_solar_positions(data: pd.DataFrame, latitude: float, longitude: float,
                           height: float = 0) -> pd.DataFrame:
    """
    Attaches the solar positions to the given data
    :param height: height of the location
    :param data: dataframe to take timestamps from and attach solar positions to
    :param latitude: latitude of the location
    :param longitude: longitude of the location
    :return: dataframe with solar positions attached
    """
    solar_pos_col_names = ['azimuth', 'elevation', 'zenith']

    if data.columns.intersection(solar_pos_col_names).size > 0:
        raise ValueError("Data already contains solar positions")

    loc = pvlib.location.Location(latitude=latitude, longitude=longitude, altitude=height)
    times = data.index
    solar_position = loc.get_solarposition(times)[solar_pos_col_names]

    data = pd.concat([data, solar_position], axis=1)

    return data


def get_dataset_from_windows(windows: list[pd.DataFrame], target: str = "Target") \
        -> ((np.array, np.array), list):
    """
    Takes a list of pandas dataframes, which are timeframes, and converts them to a dataset
    (np-array)

    :param windows: list of pandas dataframes
    :param target: Target column name
    :return: Tuple of arrays (X,y) with X being the parameters and y being the targets and a list
        of indices (datetime index). Arrays have shape (number of windows, window size, number of
        parameters)
    """

    x, y, index = [], [], []
    window_size = len(windows[0])
    columns = [col for col in windows[0].columns if col != target]

    for w in windows:
        x.append(w[columns].values.astype(np.float32))
        if target in w.columns:
            y.append(np.array(w[target].values.astype(np.float32)).reshape(window_size, 1))
        index.append(w.index.values)

    x = np.array(x)
    y = np.array(y)
    return (x, y), index


def split_windows(windows, test_ratio=0.25, weeks_in_test=1, factor=7, distinct: bool = False) \
        -> ([pd.DataFrame], [pd.DataFrame]):
    """
    Splits a list of windows (dataframes) into a train and test set.

    :param distinct: if true, train windows that overlap with test windows will be removed
    :param windows: windows to split
    :param test_ratio: ratio of test windows if test_ratio is 0.25 and there are 4 windows,
        1 will be test and 3 will be train.
    :param weeks_in_test: sets how many weeks, counting from the end of the dataset, will
        automatically be in the test set.
    :param factor: factor decides how many windows will be extracted together to minimize the
        intersection of train and test.
    :return: two lists of windows (train, test)
    """

    train, test = [], []
    test_selection = [False] * len(windows)
    max_index = max(window.index.max() for window in windows)

    for index, window in enumerate(windows):
        if window.index.max() > max_index - pd.Timedelta(weeks=weeks_in_test):
            test_selection[index] = True

    while test_selection.count(True) / len(test_selection) < test_ratio:
        random_index = random.randint(0, len(test_selection) - factor)
        for i in range(factor):
            test_selection[random_index + i] = True

    for index, test_flag in enumerate(test_selection):
        if test_flag:
            test.append(windows[index])
        else:
            train.append(windows[index])

    # if distinct is true, remove all train windows that overlap with test windows
    train_elimination = [False] * len(train)
    if distinct:
        for test_window in train:
            test_min_index = test_window.index.min()
            test_max_index = test_window.index.max()
            for index, train_window in enumerate(train):
                train_min_index = train_window.index.min()
                train_max_index = train_window.index.max()
                if ((train_min_index <= test_min_index <= test_max_index)
                        or (train_min_index <= train_max_index <= test_max_index)):
                    train_elimination[index] = True

    train = [train_window for index, train_window in enumerate(train) if
             not train_elimination[index]]

    return train, test


def windowing(data: pd.DataFrame, window_size: int = 24, stride: int = 6, max_missing: int = 6) \
        -> [pd.DataFrame]:
    """
    Creates windows from the given data (Has to be uniform data with ts index)

    :param data: dataframe with a datetime index and hourly data points
    :param window_size: size of the window in hours
    :param stride: stepover of the window in hours
    :param max_missing: maximum number of missing values in a window
    :return: list of windows (Dataframes)
    :raises ValueError: if window_size is 0, stride is 0, index has duplicates,
        max_missing > window_size, window size mismatch
    """
    if window_size <= 0:
        raise ValueError("window_size cannot be 0")
    if stride <= 0:
        raise ValueError("stride cannot be 0")

    if any(data.index.duplicated()):
        raise ValueError("cant window data with duplicated timestamps")

    if max_missing > window_size:
        logger.warning("max_missing is >= window_size, this will result in empty windows, may be a "
                       "configuration error")

    data.sort_index(inplace=True)
    min_ts = data.index.min()
    max_ts = data.index.max()

    window_lower = min_ts
    window_upper = min_ts + pd.Timedelta(hours=window_size)
    windows = []

    complete_windows = 0
    padded_windows = 0
    incomplete_windows = 0

    # fill empty timesteps with this value
    filler = 0

    while window_upper < max_ts + pd.Timedelta(hours=window_size):
        window = data.loc[window_lower:window_upper - pd.Timedelta(seconds=1)].copy()

        if len(window) == window_size:  # complete window
            complete_windows += 1
            windows.append(window.sort_index())

        # window with missing values but not too many
        elif window_size - len(window) <= max_missing:
            padded_windows += 1

            for i in pd.date_range(window_lower, window_upper - pd.Timedelta(seconds=1),
                                   freq="1h"):  # insert missing timestamps
                if i not in window.index:
                    window.loc[i] = filler
            window.sort_index(inplace=True)

            window.infer_objects(copy=False).fillna(0, inplace=True)  # fill missing values with 0
            windows.append(window)

        elif len(window) > 0:  # count incomplete windows
            incomplete_windows += 1

        window_lower += pd.Timedelta(hours=stride)
        window_upper += pd.Timedelta(hours=stride)

    for w in windows:
        if len(w) > window_size:
            raise ValueError("Window size mismatch")

    logger.info(
        f"complete windows: {complete_windows}, incomplete : {incomplete_windows}, padded: "
        f"{padded_windows}")
    return windows

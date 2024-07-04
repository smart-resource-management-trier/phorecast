"""
This file contains custom keras objects.
"""

import keras
import tensorflow as tf


@keras.saving.register_keras_serializable(package="Metrics", name="sum_difference")
def sum_difference_metric(y_true, y_pred):
    """
    This metric is used to calculate the mae of the sum in the second dimension (timesteps) of the
    output tensor.
    :param y_true: ground truth tensor
    :param y_pred: predicted tensor
    :return: tensor with a single value
    """
    return tf.math.reduce_mean(
        tf.math.abs(tf.math.reduce_sum(y_true, axis=1) - tf.math.reduce_sum(y_pred, axis=1)))


sum_difference_metric.__name__ = 'sum_difference'


@keras.saving.register_keras_serializable(package="Losses", name="sum_difference_mae")
class SumDifferenceLoss(keras.losses.Loss):
    """
    This loss is used to calculate the mean absolute error of the sum in the second dimension
    """
    __name__ = 'custom_loss'

    def __init__(self, factor: float = 2., **kwargs):
        """
        This metric is used to calculate a loss based on a weighted sum of the mae and the sum
        difference metric. The factor is used to weight the sum difference metric which is first
        devided by the number of timesteps.
        :param factor: used to weight the sum difference metric
        :param kwargs:
        """

        super().__init__(**kwargs)
        self.factor = tf.constant(factor, dtype=tf.float32)

    def call(self, y_true, y_pred):
        # get the dimension of the time axis
        time_dim = tf.cast(tf.shape(y_true)[1], tf.float32)

        # calculate the mean absolute error
        mae = tf.math.reduce_mean(tf.math.abs(y_true - y_pred), axis=-1)

        # calculate the sum difference
        sum_difference = sum_difference_metric(y_true, y_pred)
        sum_difference = tf.math.divide_no_nan(sum_difference, time_dim)

        # return the sum of the two
        return tf.math.add(mae, tf.math.multiply(sum_difference, self.factor))

    def get_config(self):
        base_config = super().get_config()
        config = {
            "factor": self.factor.numpy()
        }
        return {**base_config, **config}

    @classmethod
    def from_config(cls, config):
        factor = float(config.pop("factor"))
        return cls(factor, **config)

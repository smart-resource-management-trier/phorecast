import unittest

import numpy as np
import tensorflow as tf
from hypothesis import given, strategies as st
from hypothesis.extra.numpy import arrays
from hypothesis.strategies import composite
from keras.losses import MeanAbsoluteError
from tensorflow.python.framework.errors import InvalidArgumentError

from src.utils.keras import sum_difference_metric, SumDifferenceLoss

# Test Tensors
t1 = tf.constant([[[1.], [2.], [3.]], [[4.], [5.], [6.]]], dtype=tf.float32)
t2 = tf.constant([[[1.], [2.], [3.]], [[4.], [1.], [6.]]], dtype=tf.float32)
t3 = tf.constant([[[1.], [2.], [3.]], [[4.], [5.], [6.]], [[7.], [8.], [9.]]], dtype=tf.float32)
t4 = tf.constant([[[3.], [2.], [1.]], [[6.], [5.], [4.]], [[9.], [8.], [7.]]], dtype=tf.float32)
t5 = tf.constant([[[3.], [2.], [3.]], [[0.], [5.], [5.]], [[9.], [1.], [6.]]], dtype=tf.float32)
t6 = tf.constant([[[3.], [2.], [3.], [0.]], [[5.], [5.], [9.], [1.]], [[6.], [9.], [1.], [6.]]],
                 dtype=tf.float32)

@composite
def array_strategy(draw):
    dim_strategy = st.tuples(
        st.sampled_from([16, 32, 64, 128]),  # Width with 1 to 100
        st.integers(min_value=2, max_value=50),  # Second dimension > 1
        st.integers(min_value=1, max_value=50)  # Third dimension 1 to 100
    )
    element_strategy = st.floats(min_value=-1e6, max_value=1e6, allow_nan=False,
                                 allow_infinity=False, width=32)

    array = draw(arrays(np.float32, dim_strategy, elements=element_strategy))
    return array


class KerasTestCase(unittest.TestCase):
    def test_sum_difference_metric(self):
        self.assertEqual(sum_difference_metric(t1, t1), 0)
        self.assertEqual(sum_difference_metric(t1, t2), 2)
        self.assertEqual(sum_difference_metric(t4, t3), 0)
        self.assertEqual(sum_difference_metric(t4, t5), 5)

        # check that only 3d tensors are accepted, and tensors have the same dimensions
        self.assertRaises(InvalidArgumentError, sum_difference_metric, t1, t3)
        self.assertRaises(InvalidArgumentError, sum_difference_metric, 0, 0)
        self.assertRaises(InvalidArgumentError, sum_difference_metric, [0, 0], [0, 0])

    @given(array_strategy())
    def test_sum_difference_metric_shapes(self, array):
        tensor1 = tf.constant(array, dtype=np.float32)
        tensor2 = tf.constant((array - 1) * 2, dtype=np.float32)

        result = sum_difference_metric(tensor1, tensor1).numpy()

        self.assertEqual(result, 0.,
                         "Sum difference metric should be 0 for equal tensors")
        result = sum_difference_metric(tensor1, tensor2).numpy()

        self.assertNotEqual(result, 0.,
                            "Sum difference metric should not be 0 for unequal tensor")

    @given(array_strategy(), st.integers(min_value=1, max_value=20))
    def test_sum_difference_loss_shapes(self, array, factor):
        tensor1 = tf.constant(array, dtype=np.float32)
        tensor2 = tf.constant((array - 1) * 2, dtype=np.float32)

        loss = SumDifferenceLoss(0)
        result_c = loss(tensor1, tensor2).numpy()
        result_mae = tf.keras.losses.MeanAbsoluteError()(tensor1, tensor2).numpy()
        self.assertEqual(result_c, result_mae,
                         "Sum difference loss should be equal to MAE for factor 0")

        loss = SumDifferenceLoss(factor)
        result_c = loss(tensor1, tensor1).numpy()
        self.assertEqual(result_c, 0.0, "Sum difference loss should be 0 for equal "
                                        "tensors")

        result_c = loss(tensor1, tensor2).numpy()
        self.assertNotEqual(result_c, result_mae, "Sum difference loss should not be 0 "
                                                  "for unequal tensors")

    @given(st.integers(min_value=1, max_value=20))
    def test_sum_difference_loss(self, factor):
        loss = SumDifferenceLoss(factor)

        custom_loss_result = loss(t1, t2).numpy()
        mae = MeanAbsoluteError()(t1, t2).numpy()
        self.assertAlmostEqual(custom_loss_result, mae + (0.666666 * factor), 3)

        custom_loss_result = loss(t4, t3).numpy()
        mae = MeanAbsoluteError()(t4, t3).numpy()
        self.assertAlmostEqual(custom_loss_result, mae)

        custom_loss_result = loss(t4, t5).numpy()
        mae = MeanAbsoluteError()(t4, t5).numpy()
        self.assertAlmostEqual(custom_loss_result, mae + (1.666666 * factor), 3)

    @given(st.integers(min_value=1, max_value=20))
    def test_sum_difference_loss_serialization(self, factor):
        loss = SumDifferenceLoss(factor)
        config = loss.get_config()
        self.assertEqual(config['factor'], factor)
        loss_reconstructed = SumDifferenceLoss.from_config(config)

        loss_reconstructed_dict = loss_reconstructed.__dict__.copy()
        loss_dict = loss.__dict__.copy()

        loss_reconstructed_dict.pop("name")
        loss_dict.pop("name")

        self.assertEqual(loss_reconstructed_dict, loss_dict,
                         "Losses should be the same after serialization")


if __name__ == '__main__':
    unittest.main()

import keras.src.losses
from keras.src import ops
from keras.src.losses.loss import Loss
from keras.src.losses.loss import squeeze_or_expand_to_same_rank


class WeightedMeanAbsolutePercentageError(keras.src.losses.LossFunctionWrapper):
    """Computes the weighted mean absolute percentage error between labels and predictions.

    Args:
        reduction: Type of reduction to apply to the loss. In almost all cases
            this should be `"sum_over_batch_size"`.
            Supported options are `"sum"`, `"sum_over_batch_size"` or `None`.
        name: Optional name for the loss instance.
    """

    def __init__(
        self, reduction="sum_over_batch_size", name="weighted_mean_absolute_percentage_error"
    ):
        super().__init__(weighted_mean_absolute_percentage_error, reduction=reduction, name=name)

    def get_config(self):
        return Loss.get_config(self)


def weighted_mean_absolute_percentage_error(y_true, y_pred, eps=1e-8):
    """Computes the weighted mean absolute percentage error between labels and predictions.

    Args:
        y_true: Ground truth values with shape = `[batch_size, d0, .. dN]`.
        y_pred: The predicted values with shape = `[batch_size, d0, .. dN]`.

    Returns:
        weighted mean absolute percentage error values with shape = `[batch_size, d0, .. dN-1]`.

    """

    y_pred = ops.convert_to_tensor(y_pred)
    y_true = ops.convert_to_tensor(y_true, dtype=y_pred.dtype)
    y_true, y_pred = squeeze_or_expand_to_same_rank(y_true, y_pred)

    nominator = ops.sum(ops.abs(y_true - y_pred), axis=-1)
    deliminator = ops.sum(ops.abs(y_true), axis=-1)

    return nominator / (deliminator + eps)

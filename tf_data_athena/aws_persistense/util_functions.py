import numpy as np
import tensorflow as tf

from tf_data_athena.aws_persistense.athena_dataset import AthenaDataset


def create_athena_dataset_from_connector(query, athena_connector,
                                         num_parallel_calls=1, **kwargs):
    raw_dataset = AthenaDataset(query, athena_connector, **kwargs)
    record_defaults = _make_record_defaults(raw_dataset.column_types)

    # Skip header and parse CSV lines
    return raw_dataset.skip(1).map(lambda u: tf.io.decode_csv(u, record_defaults, na_value=""), num_parallel_calls)


@tf.function
def _make_record_defaults(column_types):
    return tuple([_default_for_type(dtype) for dtype in column_types])


@tf.function
def _default_for_type(dtype):
    if dtype == tf.string:
        return tf.constant(value=[""], shape=(1,), dtype=dtype)

    return tf.constant(value=[np.nan], shape=(1,), dtype=tf.float32)

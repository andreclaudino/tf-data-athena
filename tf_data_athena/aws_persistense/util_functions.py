import tensorflow as tf
from tf_data_athena.aws_persistense.athena_dataset import AthenaDataset


def create_athena_dataset_from_connector(query, athena_connector,
                                         num_parallel_calls=tf.data.experimental.AUTOTUNE, **kwargs):
    raw_dataset = AthenaDataset(query, athena_connector, **kwargs)
    record_defaults = _make_record_defaults(raw_dataset.column_types)

    # Skip header and parse CSV lines
    return raw_dataset.skip(1).map(lambda u: tf.io.decode_csv(u, record_defaults), num_parallel_calls)


@tf.function
def _make_record_defaults(column_types):
    return tuple([_default_for_type(dtype) for dtype in column_types])


@tf.function
def _default_for_type(dtype):
    return tf.constant((), shape=(0,), dtype=dtype)
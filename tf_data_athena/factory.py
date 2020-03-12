from tf_data_athena.aws_persistense.athena_connector import AthenaConnector
from tf_data_athena.aws_persistense.util_functions import create_athena_dataset_from_connector
from tf_data_athena.aws_persistense.athena_dataset import AthenaDataset
import tensorflow as tf


def create_athena_dataset(query: str, s3_output_location: str, waiting_interval: float = 1.0,
                          num_parallel_calls=tf.data.experimental.AUTOTUNE, **kwargs) -> AthenaDataset:
    athena_connector = AthenaConnector(s3_output_location, waiting_interval)

    return create_athena_dataset_from_connector(query, athena_connector, num_parallel_calls, **kwargs)

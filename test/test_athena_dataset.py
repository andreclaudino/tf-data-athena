import unittest

import numpy as np
import tensorflow as tf

from test.mocks.mock_athena_connector import MockAthenaConnector
from tf_data_athena.aws_persistense.util_functions import create_athena_dataset_from_connector


class TestAthenaDataset(unittest.TestCase):

    def test_should_parse_file_and_return_first_batches(self):
        output_path = "resources/sample.csv"
        columns_data = (("root", tf.string), ("group", tf.int32), ("status", tf.string), ("date", tf.string))

        query = """
            select
                root,
                group,
                status,
                date
            from
                ia_front.tagged_visits
            limit 100
        """

        connector = MockAthenaConnector().set_output_file(output_path).set_columns_data(columns_data)

        dataset = create_athena_dataset_from_connector(query, connector, num_parallel_calls=1)
        actuals = list(dataset.batch(3).take(2).as_numpy_iterator())[:2]

        actual0 = actuals[0]
        actual1 = actuals[1]

        expected0 = (np.array([b'false', b'false', b'false'], dtype=object),
                     np.array([2, 20, 21], dtype=np.int32),
                     np.array([b'validate', b'opened', b'opeded'], dtype=object),
                     np.array([b'2020-01-10', b'2020-01-10', b'2020-01-10'], dtype=object))

        expected1 = (np.array([b'true', b'false', b'false'], dtype=object),
                     np.array([1, 3, 106], dtype=np.int32),
                     np.array([b'validate', b'opened', b'opeded'], dtype=object),
                     np.array([b'2020-01-10', b'2020-01-09', b'2020-01-10'], dtype=object))

        assert np.array_equal(actual0, expected0)
        assert np.array_equal(actual1, expected1)

    def test_should_parse_file_with_voids_and_return_firsts_batches(self):
        output_path = "resources/sample_with_void.csv"
        columns_data = (("root", tf.string), ("group", tf.int32), ("status", tf.string), ("date", tf.string))

        query = """
            select
                root,
                group,
                status,
                date
            from
                ia_front.tagged_visits
            limit 100
        """

        connector = MockAthenaConnector().set_output_file(output_path).set_columns_data(columns_data)

        dataset = create_athena_dataset_from_connector(query, connector, num_parallel_calls=1)

        actuals = list(dataset.batch(3).take(2).as_numpy_iterator())[:2]

        actual0 = actuals[0]
        actual1 = actuals[1]

        expected0 = (np.array([b'false', b'false', b''], dtype=object),
                     np.array([ 2., np.nan, 21.], dtype=np.float32),
                     np.array([b'validate', b'opened', b'opeded'], dtype=object),
                     np.array([b'', b'2020-01-10', b'2020-01-10'], dtype=object))

        expected1 = (np.array([b'true', b'false', b'false'], dtype=object),
                     np.array([1.0, np.nan, 106.0], dtype=np.float32),
                     np.array([b'validate', b'opened', b''], dtype=object),
                     np.array([b'2020-01-10', b'2020-01-09', b'2020-01-10'], dtype=object))

        np.testing.assert_equal(actual0, expected0, verbose=True)
        np.testing.assert_equal(actual1, expected1, verbose=True)


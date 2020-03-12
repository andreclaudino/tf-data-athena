import logging
from typing import Tuple

import tensorflow as tf

from tf_data_athena.aws_persistense.athena_connector import AthenaConnector

logging.basicConfig(format='[%(asctime)s]:[%(levelname)s]:[%(message)s]', level=logging.INFO)


class AthenaDataset(tf.data.TextLineDataset):

    def __init__(self,
                 query: str,
                 athena_connector: AthenaConnector,
                 **kwargs):

        self.query: str = query
        self.athena_client: AthenaConnector = athena_connector

        self._file_path = None

        self.column_types: Tuple = tuple()
        self.column_names: Tuple = tuple()
        self.row_shape: Tuple = tuple()

        self._load()

        super().__init__(self._file_path, **kwargs)

    def _load(self):
        query_metadata = self.athena_client.submit_query(self.query)
        self._file_path = query_metadata["output_file"]

        self.column_names = tuple([name for name, _ in query_metadata['column_types']])
        self.column_types = tuple([column_type for _, column_type in query_metadata['column_types']])
        self.row_shape = (len(self.column_types),)

        logging.info(f"setting file result from query: {self._file_path}")

import logging
import time
import uuid
from typing import Dict, Sequence, Tuple, List

import boto3
import tensorflow as tf

logging.basicConfig(format='[%(asctime)s]:[%(levelname)s]:[%(message)s]', level=logging.INFO)


def _execution_status(response):
    return response['QueryExecution']['Status']['State']


def _make_error_message(response):
    status = response['QueryExecution']['Status']

    state = status['State']
    reason = status['StateChangeReason']
    submission = status['SubmissionDateTime']
    completion = status['CompletionDateTime']
    query_id = status["QueryExecutionId"]

    return f"Query {query_id} stated executing at {str(submission)} and change to status {state} at " \
           f"{str(completion)} with message {reason}"



def _output_file(response):
    return response["QueryExecution"]["ResultConfiguration"]["OutputLocation"]


def _parse_columns_types(columns_info: Sequence[Dict]) -> List[Tuple[str, tf.DType]]:
    return [_parse_column_type(_) for _ in columns_info]


def _parse_column_type(column_info: Dict) -> Tuple[str, tf.DType]:
    column_name = column_info["Name"]
    column_type = column_info["Type"].upper()

    if column_type in ["BOOLEAN"]:
        return column_name, tf.string

    if column_type in ["TINYINT", "SMALLINT", "INT", "INTEGER"]:
        return column_name, tf.int32

    if column_type in ["BIGINT"]:
        return column_name, tf.int64

    if column_type in ["DOUBLE"] or column_type.startswith("DECIMAL"):
        return column_name, tf.float64

    if column_type in ["REAL"]:
        return column_name, tf.float64

    if column_type.startswith("CHAR") or column_type.startswith("VARCHAR"):
        return column_name, tf.string

    if column_type in ["DATE"]:
        return column_name, tf.string

    if column_type in ["TIMESTAMP"]:
        return column_name, tf.string

    return column_name, tf.string


class AthenaConnector:

    def __init__(self, s3_output_location: str, waiting_interval: float):
        self.s3_output_location = s3_output_location
        self.waiting_time = waiting_interval

    def submit_query(self, query: str) -> dict:
        athena_client = boto3.client('athena')

        query_execution_id = athena_client.start_query_execution(
            QueryString=query,
            ClientRequestToken=str(uuid.uuid4()),
            ResultConfiguration={'OutputLocation': self.s3_output_location,
                                 'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}})['QueryExecutionId']

        status = 'QUEUED'

        while status not in ['SUCCEEDED', 'FAILED', 'CANCELLED']:

            response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = _execution_status(response)

            if status == 'SUCCEEDED':
                logging.info("Query succeeded")
                query_results = athena_client.get_query_results(QueryExecutionId=query_execution_id, MaxResults=1)

                column_info = query_results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
                results_data = _parse_columns_types(column_info)

                output_file = _output_file(response)
                return dict(output_file=output_file, column_types=results_data)

            if status in ['FAILED', 'CANCELLED']:
                logging.error(_make_error_message(response))
                raise Exception(f"Query failed to execute. status={status}")

            logging.info(f'Query ({query_execution_id}) actual status is {status}')
            time.sleep(self.waiting_time)




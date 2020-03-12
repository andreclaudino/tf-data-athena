from tf_data_athena.aws_persistense.athena_connector import AthenaConnector


class MockAthenaConnector(AthenaConnector):

    def __init__(self):
        self.output_file = ""
        self.columns_data = tuple()

    def set_output_file(self, output_file):
        self.output_file = output_file
        return self

    def set_columns_data(self, columns_data):
        self.columns_data = columns_data
        return self

    def submit_query(self, query: str) -> dict:
        return dict(output_file=self.output_file, column_types=self.columns_data)
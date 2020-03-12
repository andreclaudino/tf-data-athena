# Tensorflow Data for AWS Athena

An AWS athena library for `tensorflow.data.Dataset`. If you don't know `tf.data`, take a look at [documentation](https://www.tensorflow.org/api_docs/python/tf/data/Dataset) and this [example](https://www.tensorflow.org/guide/data).

## How to use
Use is almost as simple as another tf.Dataset implementation. You just need to create a dataset using the funciton `create_athena_dataset`

no (it follows [aws authentication chain in boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/security.html#identity-and-access-management-intro)).  

```PYTHON
# imports
from tf_data_athena import create_athena_dataset

# connector parameters
s3_output_location = "s3://my-bucket/my-folder/athena-outputs" # Athena output bucket folder
waiting_interval = 0.1 # Time (in seconds) to wait before asking for query state

# query
query = "select * from my_namespace.my_table"

# create dataset
dataset = create_athena_dataset(query, s3_output_location)
```

Now, `dataset` is an instance of `tf.data.Dataset` containing query results.

## Parameters

Then factory funcion `create_athena_dataset` has the following parameters:

* `query`: The query to be ran in athena
* `s3_output_location`: An s3 path with write access for the current account where the query results file will be saved
* `waiting_interval`: A float number representing the number of seconds between to wait before ask for query status on athena
* `num_parallel_calls`: Argument for `tf.data.Dataset.map` (see documentation) while parsing result rows
* *other named arguments*: Any other named argument will be used on `tf.data.TextLineDataset` constructor, please, see documentation.

## AWS Authorization
This library uses [`boto3`](https://github.com/boto/boto3) behind the scenes, then, it follows the same authentication/authorization chain.
Authorized user or service needs permission to *create and execute athena queries* and *create and read s3 objects* in the folder defined by `s3_output_location`.
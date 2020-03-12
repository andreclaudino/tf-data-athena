from setuptools import setup

setup(
    name='aws_persistense-tf-dataset',
    version='',
    packages=['tf_athena_dataset', 'tf_athena_dataset.aws_persistense', 'tf_athena_dataset.dataset'],
    url='',
    license='',
    author='Time IA Front',
    author_email='',
    description='',
    install_requires=[
        'tensorflow_datasets==2.1.0',
        'tensorflow-gpu==2.1.0',
        'boto3==1.12.17'
    ]
)

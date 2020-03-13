import pathlib

from setuptools import setup, find_packages

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()

setup(
    name='tf-data-athena',
    version='1.0.1',
    packages=find_packages(),
    license='MIT',
    author='André Claudino',
    url="https://github.com/andreclaudino/tf-data-athena",
    author_email='',
    long_description_content_type="text/markdown",
    long_description=README,
    include_package_data=True,
    description='An implementation of tf.data.Dataset for aws Athena',
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
    setup_requires=['wheel', 'twine'],
    install_requires=[
        'tensorflow_datasets==2.1.0',
        'tensorflow==2.1.0',
        'boto3==1.12.17'
    ]
)

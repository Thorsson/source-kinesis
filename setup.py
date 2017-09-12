try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name="panoply_kinesis",
    version="1.0.0",
    description="Panoply Data Source for the AWS Kinesis Stream",
    author="Ivan Turkovic",
    author_email="ivan.turkovic@gmail.com",
    url="http://panoply.io",
    install_requires=[
        "panoply-python-sdk",
        "boto3"
    ],
    package_dir={"panoply": ""},
    packages=[
        "panoply.kinesis"
    ]
)

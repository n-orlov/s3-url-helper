import os
import pathlib
from setuptools import setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()
LICENSE = (HERE / "LICENSE").read_text()

version = "0.2"
artifact_name = "s3_url_helper"

setup(
    name=artifact_name,
    version=version,
    description="S3 Url Helper",
    long_description=README,
    long_description_content_type="text/markdown",
    license=LICENSE,
    url="https://github.com/n-orlov/s3-url-helper",
    author="Nikolai Orlov",
    author_email="nikolaiorl@gmail.com",
    packages=["s3_url"],
    include_package_data=True,
    install_requires=["boto3"],
    extras_require={
        'dev': [
            'pytest',
            'pytest-cov',
            'moto',
            'pylint',
            'assertpy',
            'twine',
            'build'
        ]
    }
)
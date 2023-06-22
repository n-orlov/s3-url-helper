import os
from pathlib import Path
from unittest import mock

import boto3
import pytest
from assertpy import assert_that
from assertpy.assertpy import AssertionBuilder
from moto import mock_s3
from typing import Callable
from datetime import datetime
import time
from s3_url import S3Url

TEST_BUCKET = 'test-bucket'
DEFAULT_REGION = 'us-east-1'


@pytest.fixture(autouse=True, scope='function')
def aws_defaults():
    """Mocked AWS Credentials & Region for moto."""
    with mock.patch.dict(os.environ, {
        'AWS_ACCESS_KEY_ID': 'testing',
        'AWS_SECRET_ACCESS_KEY': 'testing',
        'AWS_SECURITY_TOKEN': 'testing',
        'AWS_SESSION_TOKEN': 'testing',
        'AWS_DEFAULT_REGION': DEFAULT_REGION
    }):
        yield


@pytest.fixture(autouse=True)
def s3_moto():
    with mock_s3():
        S3Url._s3_res = boto3.resource('s3')
        yield


@pytest.fixture
def s3_test_bucket(s3_moto):
    # just for demo - create 'inbound' bucket
    s3 = boto3.resource('s3')
    bucket = s3.create_bucket(Bucket=TEST_BUCKET)
    yield bucket


def s3_file_fixture(name, key, filename):
    @pytest.fixture
    def _s3_file_fixture(s3_test_bucket):
        current_path = Path(__file__).resolve().parent
        img_path = current_path / "resources" / filename
        with img_path.open(mode='rb') as fobj:
            s3_test_bucket.upload_fileobj(Fileobj=fobj, Key=key)
        yield key

    globals()[name] = _s3_file_fixture


s3_file_fixture('s3_demo_file', 'prefix/file.json', 'test_file.json')
s3_file_fixture('s3_test_file', 'SomeFolder/test_file.json', 'test_file.json')
s3_file_fixture('s3_test_file_2', 'SomeFolder/test_file_2.json', 'test_file.json')
s3_file_fixture('s3_test_file_3', 'a/path/to/the/file/test_file_3.json', 'test_file.json')


def assert_with_timeout(predicate: Callable, timeout_sec=90, poll_interval=1):
    started_at = datetime.now()
    while True:
        try:
            return predicate()
        except AssertionError as exc:
            time.sleep(poll_interval)
            if (datetime.now() - started_at).total_seconds() > timeout_sec:
                raise exc


def has_s3_tags_equal_to(self, expected_tags: dict):
    target_tags = S3Url(self.val).read_tags()
    assert_that(target_tags).is_equal_to(expected_tags)

    return self


def has_s3_tags_contains_entry(self, *args, **kwargs):
    target_tags = get_s3_url_obj(self).read_tags()
    assert_that(target_tags).contains_entry(*args, **kwargs)

    return self


def s3_file_exists(self):
    exists = get_s3_url_obj(self).exists()
    assert_that(exists).is_true()

    return self


def s3_file_does_not_exist(self):
    exists = get_s3_url_obj(self).exists()
    assert_that(exists).is_false()

    return self


def get_s3_url_obj(self) -> S3Url:
    return self.val if isinstance(self.val, S3Url) else S3Url(self.val)


AssertionBuilder.has_s3_tags_equal_to = has_s3_tags_equal_to
AssertionBuilder.has_s3_tags_contains_entry = has_s3_tags_contains_entry
AssertionBuilder.s3_file_exists = s3_file_exists
AssertionBuilder.s3_file_does_not_exist = s3_file_does_not_exist

import json
import os
from pathlib import Path
from unittest.mock import patch

import boto3
import pytest
import requests
from assertpy import assert_that
from boto3 import s3
from botocore.exceptions import ClientError
from s3_url import S3Url
from tests.conftest import TEST_BUCKET, assert_with_timeout


def test_demo(s3_test_bucket, s3_demo_file):
    file_url = S3Url(f's3://test-bucket/prefix/file.json')

    # url component properties
    assert file_url.bucket == 'test-bucket'
    assert file_url.key == 'prefix/file.json'

    # factory methods
    file_url = S3Url.from_url('s3://test-bucket/prefix/file.json')
    file_url = S3Url.from_bucket_key(bucket='test-bucket', key='prefix/file.json')

    # access underlying boto3 s3 resource object
    boto3_obj = file_url.object

    # return url string
    url_str: str = file_url.url
    url_str: str = str(file_url)
    assert url_str == 's3://test-bucket/prefix/file.json'

    # check if file exists
    exists: bool = file_url.exists()
    assert exists

    # check if any files exist in prefix (url should end with /)
    prefix_exists = S3Url('s3://test-bucket/prefix/').prefix_exists()
    assert prefix_exists

    # read text/json
    file_content_bin: bytes = file_url.read()
    file_content_text: str = file_url.read_text()
    file_content_json: json = file_url.read_json()

    # delete file
    file_url.delete()
    assert not file_url.exists()

    # write text/json
    file_url.write_text("test data")
    file_url.write_json({"testEntry": "test data"})
    assert file_url.exists()

    # copy to another object
    file_url.copy_to('s3://test-bucket/prefix/file-copy.json')
    S3Url('s3://test-bucket/prefix/another-file-copy.json').copy_from(file_url)

    # delete all filed in prefix
    prefix_url = S3Url('s3://test-bucket/prefix/')
    prefix_url.delete_dir()

    assert not prefix_url.prefix_exists()
    assert not file_url.exists()


def test_s3_url_parts(s3_test_bucket, s3_test_file):
    url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file}')
    assert_that(url.url).is_equal_to(f's3://{TEST_BUCKET}/SomeFolder/test_file.json')
    assert_that(url.bucket).is_equal_to(TEST_BUCKET)
    assert_that(url.key).is_equal_to('SomeFolder/test_file.json')


def test_factory_methods(s3_test_bucket, s3_test_file):
    url = S3Url.from_url(f's3://{s3_test_bucket.name}/{s3_test_file}')
    assert_that(url.url).is_equal_to(f's3://{TEST_BUCKET}/SomeFolder/test_file.json')
    assert_that(url.bucket).is_equal_to(TEST_BUCKET)
    assert_that(url.key).is_equal_to('SomeFolder/test_file.json')

    url = S3Url.from_bucket_key(s3_test_bucket.name, s3_test_file)
    assert_that(url.url).is_equal_to(f's3://{TEST_BUCKET}/SomeFolder/test_file.json')
    assert_that(url.bucket).is_equal_to(TEST_BUCKET)
    assert_that(url.key).is_equal_to('SomeFolder/test_file.json')


def test_unsupported_url(s3_test_bucket):
    with pytest.raises(Exception) as err:
        S3Url(f'https://{s3_test_bucket.name}/some/path')
    assert_that(str(err)).contains('Unsupported URL')


def test_s3_url_exists(s3_test_bucket, s3_test_file):
    existing_url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file}')
    assert_that(existing_url.exists()).is_true()
    non_existing_url = S3Url(f's3://{s3_test_bucket.name}/non_existing_file.txt')
    assert_that(non_existing_url.exists()).is_false()


def test_generate_presigned_url_get(s3_test_bucket, s3_test_file):
    existing_url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file}')
    get_url = existing_url.generate_presigned_url_get()
    assert_that(requests.get(get_url).content.decode()).is_equal_to(existing_url.read_text())


def test_generate_presigned_url_get_with_region(s3_test_bucket, s3_test_file):
    with patch.dict(os.environ, {'AWS_REGION': 'us-east-1'}):
        if hasattr(S3Url._local, 's3_res'):
            delattr(S3Url._local, "s3_res")
        existing_url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file}')
        get_url = existing_url.generate_presigned_url_get()
        assert_that(get_url).contains('s3.us-east-1.amazonaws.com')
        assert_that(requests.get(get_url).content.decode()).is_equal_to(existing_url.read_text())


def test_generate_presigned_url_put(s3_test_bucket, s3_test_file):
    non_existing_url = S3Url(f's3://{s3_test_bucket.name}/non_existing_file.txt')
    get_url = non_existing_url.generate_presigned_url_put()
    requests.put(get_url, data="test text file".encode()).raise_for_status()
    assert_that(non_existing_url.read_text()).is_equal_to("test text file")

def test_generate_presigned_url_put_w_headers(s3_test_bucket, s3_test_file):
    non_existing_url = S3Url(f's3://{s3_test_bucket.name}/non_existing_file.txt')
    get_url = non_existing_url.generate_presigned_url_put(ContentType='application/json')
    requests.put(get_url, data="test text file".encode(), headers={"Content-Type": "application/json"}).raise_for_status()
    assert_that(non_existing_url.read_text()).is_equal_to("test text file")


def test_s3_url_exists_access_denied(s3_test_bucket, s3_test_file, monkeypatch):
    existing_url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file}')
    expected_exc = ClientError({'Error': {'Code': '403', 'Message': 'Forbidden'},
                                'ResponseMetadata': {'HTTPStatusCode': 403, 'RetryAttempts': 0}}, 'HeadObject')

    def raise_exc(*args, **kwargs):
        raise expected_exc

    monkeypatch.setattr(existing_url.object.meta.client, '_make_api_call', raise_exc)
    assert_that(existing_url.exists).raises(ClientError).when_called_with()


def test_s3_url_exists_for_a_path(s3_test_bucket, s3_test_file_3):
    existing_url = S3Url(f's3://{s3_test_bucket.name}/a/path/to/')
    assert_that(existing_url.prefix_exists()).is_true()
    existing_url = S3Url(f's3://{s3_test_bucket.name}/a/path/to/the/file')
    assert_that(existing_url.prefix_exists()).is_true()

    non_existing_url = S3Url(f's3://{s3_test_bucket.name}/unknown/path/to/the/file')
    assert_that(non_existing_url.prefix_exists()).is_false()


def test_list_prefix_objects(s3_test_bucket):
    file_1 = S3Url(f's3://{s3_test_bucket.name}/some_prefix/some_file_1.txt')
    file_2 = S3Url(f's3://{s3_test_bucket.name}/some_prefix/some_file_2.txt')
    file_3 = S3Url(f's3://{s3_test_bucket.name}/some_prefix/some_file_3.txt')

    file_1.write_text("1")
    file_2.write_text("2")
    file_3.write_text("3")

    existing_prefix_url = S3Url(f's3://{s3_test_bucket.name}/some_prefix/')
    assert_that(list(existing_prefix_url.list_prefix_objects())).is_length(3).contains_only(file_1, file_2, file_3)


def test_list_prefix_objects_non_existing_prefix(s3_test_bucket):
    existing_prefix_url = S3Url(f's3://{s3_test_bucket.name}/non-existing/')
    assert_that(list(existing_prefix_url.list_prefix_objects())).is_empty()


def test_list_common_prefixes(s3_test_bucket):
    S3Url(f's3://{s3_test_bucket.name}/some_prefix/sub1/some_file_1.txt').write_text("test")
    S3Url(f's3://{s3_test_bucket.name}/some_prefix/sub1/some_file_2.txt').write_text("test")
    S3Url(f's3://{s3_test_bucket.name}/some_prefix/sub2/some_file_3.txt').write_text("test")
    S3Url(f's3://{s3_test_bucket.name}/some_prefix/sub3/sub4/some_file_3.txt').write_text("test")
    assert_that(list(S3Url(f's3://{s3_test_bucket.name}/some_prefix/sub1/').list_common_prefixes())).is_empty()
    assert_that(list(S3Url(f's3://{s3_test_bucket.name}/some_prefix/sub2/').list_common_prefixes())).is_empty()
    assert_that(list(S3Url(f's3://{s3_test_bucket.name}/some_prefix/sub3/').list_common_prefixes())).is_length(1) \
        .contains(S3Url('s3://test-bucket/some_prefix/sub3/sub4/'))
    assert_that(list(S3Url(f's3://{s3_test_bucket.name}/some_prefix/').list_common_prefixes())) \
        .is_length(3) \
        .contains(
        S3Url('s3://test-bucket/some_prefix/sub1/'),
        S3Url('s3://test-bucket/some_prefix/sub2/'),
        S3Url('s3://test-bucket/some_prefix/sub3/'),
    )


def test_list_common_prefixes_empty(s3_test_bucket):
    res = list(S3Url(f's3://{s3_test_bucket.name}/non-existing/').list_common_prefixes())
    assert_that(res).is_empty()


def test_s3_url_exists_for_a_path_access_denied(s3_test_bucket, s3_test_file_3, monkeypatch):
    existing_url = S3Url(f's3://{s3_test_bucket.name}/a/path/to/')
    expected_exc = ClientError({'Error': {'Code': '403', 'Message': 'Forbidden'},
                                'ResponseMetadata': {'HTTPStatusCode': 403, 'RetryAttempts': 0}}, 'ListObjects')

    def raise_exc(*args, **kwargs):
        raise expected_exc

    monkeypatch.setattr(existing_url.object.meta.client, '_make_api_call', raise_exc)
    assert_that(existing_url.prefix_exists).raises(ClientError).when_called_with()


def test_s3_url_delete(s3_test_bucket, s3_test_file):
    existing_url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file}')
    assert_that(existing_url.exists()).is_true()
    existing_url.delete()
    assert_that(existing_url.exists()).is_false()
    # should not fail if not exists
    existing_url.delete()


def test_s3_url_delete_dir(s3_test_bucket, s3_test_file):
    existing_url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file}')
    assert_that(existing_url.exists()).is_true()
    prefix = S3Url(f's3://{s3_test_bucket.name}/SomeFolder/')
    prefix.delete_dir()
    assert_that(existing_url.exists()).is_false()


def test_s3_url_read_write(s3_test_bucket, s3_test_file):
    url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file}')
    assert_that(url.read().replace(b'\r\n', b'\n')).is_equal_to(b'{\n  "testEntry1": "value1"\n}')
    assert_that(json.loads(url.read_text())).is_equal_to({
        "testEntry1": "value1"
    })
    url.write_text(json.dumps({
        "newField": "newValue"
    }))
    assert_that(url.object.server_side_encryption).is_none()

    assert_that(json.loads(url.read_text())).is_equal_to({
        "newField": "newValue"
    })
    url.write_text(json.dumps({
        "newField": "newValue"
    }), encryption='AES256')
    assert_that(url.object.server_side_encryption).is_equal_to('AES256')


def test_s3_url_read_write_json(s3_test_bucket, s3_test_file):
    url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file}')
    assert_that(url.read_json()).is_equal_to({
        "testEntry1": "value1"
    })
    url.write_json({
        "newField": "newValue"
    })
    assert_that(url.object.server_side_encryption).is_none()

    assert_that(url.read_json()).is_equal_to({
        "newField": "newValue"
    })
    url.write_json({
        "newField": "newValue"
    }, encryption='AES256')
    assert_that(url.object.server_side_encryption).is_equal_to('AES256')


def test_s3_url_read_non_existing(s3_test_bucket):
    non_existing_url = S3Url(f's3://{s3_test_bucket.name}/non_existing_file.txt')
    with pytest.raises(ClientError) as err:
        non_existing_url.read_text()
    assert_that(str(err.value)).contains('NoSuchKey')
    assert_that(str(err.value)).contains('The specified key does not exist')


def test_s3_url_upload_file(s3_test_bucket):
    url = S3Url(f's3://{s3_test_bucket.name}/some_new_file.json')
    current_path = Path(__file__).resolve().parent
    img_path = current_path / "resources" / "test_file.json"
    with img_path.open(mode='rb') as fobj:
        url.upload_file(fobj)
    assert_that(json.loads(url.read_text())).is_equal_to({
        "testEntry1": "value1"
    })
    assert_that(url.object.server_side_encryption).is_none()


def test_s3_url_upload_file_encryption(s3_test_bucket):
    url = S3Url(f's3://{s3_test_bucket.name}/some_new_file.json')
    current_path = Path(__file__).resolve().parent
    img_path = current_path / "resources" / "test_file.json"
    with img_path.open(mode='rb') as fobj:
        url.upload_file(fobj, encryption='AES256')
    assert_that(json.loads(url.read_text())).is_equal_to({
        "testEntry1": "value1"
    })
    assert_that(url.object.server_side_encryption).is_equal_to('AES256')


def test_copy_to_url(s3_test_bucket, s3_test_file):
    # Given
    source_url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file}')
    target_url = S3Url(f's3://{s3_test_bucket.name}/copied/{s3_test_file}')
    assert_that(target_url).s3_file_does_not_exist()

    # When
    source_url.copy_to(target_url)

    # Then
    assert_that(target_url).s3_file_exists()


def test_copy_to_str(s3_test_bucket, s3_test_file):
    # Given
    source_url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file}')
    target_url = S3Url(f's3://{s3_test_bucket.name}/copied/{s3_test_file}')
    assert_that(target_url).s3_file_does_not_exist()

    # When
    source_url.copy_to(target_url.url)

    # Then
    assert_that(target_url).s3_file_exists()


def test_copy_from_url(s3_test_bucket, s3_test_file):
    # Given
    source_url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file}')
    target_url = S3Url(f's3://{s3_test_bucket.name}/copied/{s3_test_file}')
    assert_that(target_url).s3_file_does_not_exist()

    # When
    target_url.copy_from(source_url)

    # Then
    assert_that(target_url).s3_file_exists()


def test_copy_from_str(s3_test_bucket, s3_test_file):
    # Given
    source_url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file}')
    target_url = S3Url(f's3://{s3_test_bucket.name}/copied/{s3_test_file}')
    assert_that(target_url).s3_file_does_not_exist()

    # When
    target_url.copy_from(source_url.url)

    # Then
    assert_that(target_url).s3_file_exists()


def test_should_read_tags_to_s3_file(s3_test_bucket, s3_test_file):
    # Given
    url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file}')
    s3_test_bucket.meta.client.put_object_tagging(
        Bucket=s3_test_bucket.name,
        Key=s3_test_file,
        Tagging={
            'TagSet': [
                {'Key': 'tag1', 'Value': 'value1'},
                {'Key': 'tag2', 'Value': 'value2'},
            ]
        }
    )

    # Then
    assert_that(url).has_s3_tags_equal_to({'tag1': 'value1', 'tag2': 'value2'})


def test_should_add_and_read_no_tags_to_s3_file(s3_test_bucket, s3_test_file):
    # Given
    input_tags = {}
    url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file}')

    # When
    url.write_tags(input_tags)

    # Then
    assert_that(url).has_s3_tags_equal_to({})


def test_should_add_and_read_none_tags_to_s3_file(s3_test_bucket, s3_test_file):
    # Given
    input_tags = None

    url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file}')

    # When
    url.write_tags(input_tags)

    # Then
    assert_that(url).has_s3_tags_equal_to({})


def test_should_copy_s3_file_with_tags(s3_test_bucket, s3_test_file):
    # Given
    source_s3_url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file}')
    target_s3_url = S3Url(f's3://{s3_test_bucket.name}/copied/{s3_test_file}')
    s3_test_bucket.meta.client.put_object_tagging(
        Bucket=s3_test_bucket.name,
        Key=s3_test_file,
        Tagging={
            'TagSet': [
                {'Key': 'tag1', 'Value': 'value1'},
                {'Key': 'tag2', 'Value': 'value2'},
            ]
        }
    )

    # When
    source_s3_url.copy_to(target_s3_url)

    # Then
    assert_that(target_s3_url).s3_file_exists()
    assert_that(target_s3_url).has_s3_tags_equal_to({'tag1': 'value1', 'tag2': 'value2'})


def test_should_copy_tags_to_url(s3_test_bucket, s3_test_file, s3_test_file_2):
    # Given
    source_url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file}')
    target_url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file_2}')

    assert_that(source_url).has_s3_tags_equal_to({})
    assert_that(target_url).has_s3_tags_equal_to({})

    # When
    source_url.write_tags({'tag1': 'value1', 'tag2': 'value2'})
    source_url.copy_tags_to(target_url)

    # Then
    assert_that(target_url).s3_file_exists()
    assert_that(target_url).has_s3_tags_equal_to({'tag1': 'value1', 'tag2': 'value2'})


def test_should_copy_tags_to_str(s3_test_bucket, s3_test_file, s3_test_file_2):
    # Given
    source_url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file}')
    target_url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file_2}')

    assert_that(source_url).has_s3_tags_equal_to({})
    assert_that(target_url).has_s3_tags_equal_to({})

    # When
    source_url.write_tags({'tag1': 'value1', 'tag2': 'value2'})
    source_url.copy_tags_to(target_url.url)

    # Then
    assert_that(target_url).has_s3_tags_equal_to({'tag1': 'value1', 'tag2': 'value2'})


def test_should_copy_tags_from_url(s3_test_bucket, s3_test_file, s3_test_file_2):
    # Given
    source_url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file}')
    target_url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file_2}')

    assert_that(source_url).has_s3_tags_equal_to({})
    assert_that(target_url).has_s3_tags_equal_to({})

    # When
    source_url.write_tags({'tag1': 'value1', 'tag2': 'value2'})
    target_url.copy_tags_from(source_url)

    # Then
    assert_that(target_url).s3_file_exists()
    assert_that(target_url).has_s3_tags_equal_to({'tag1': 'value1', 'tag2': 'value2'})


def test_should_copy_tags_from_str(s3_test_bucket, s3_test_file, s3_test_file_2):
    # Given
    source_url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file}')
    target_url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file_2}')

    assert_that(source_url).has_s3_tags_equal_to({})
    assert_that(target_url).has_s3_tags_equal_to({})

    # When
    source_url.write_tags({'tag1': 'value1', 'tag2': 'value2'})
    target_url.copy_tags_from(source_url.url)

    # Then
    assert_that(target_url).s3_file_exists()
    assert_that(target_url).has_s3_tags_equal_to({'tag1': 'value1', 'tag2': 'value2'})


def test_should_copy_s3_file_with_no_tags(s3_test_bucket, s3_test_file, s3_test_file_2):
    # Given
    source_url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file}')
    target_url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file_2}')

    assert_that(source_url).has_s3_tags_equal_to({})
    assert_that(target_url).has_s3_tags_equal_to({})

    # When
    source_url.copy_tags_to(target_url)

    # Then
    assert_that(target_url.exists()).is_true()
    assert_that(target_url).has_s3_tags_equal_to({})


def test_transition_to_glacier(s3_test_bucket, s3_test_file):
    # Given
    source_url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file}')

    # When
    source_url.transition_to_storage_tier("GLACIER")

    # Then
    assert_that(source_url.object.storage_class).is_equal_to("GLACIER")


def test_shouldnt_transition_from_glacier(s3_test_bucket, s3_test_file):
    # Given
    source_url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file}')
    source_url.transition_to_storage_tier("GLACIER")

    # When
    with pytest.raises(Exception):
        source_url.transition_to_storage_tier("STANDARD")


def test_transition_from_int_tiering(s3_test_bucket, s3_test_file):
    # Given
    source_url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file}')
    source_url.transition_to_storage_tier("INTELLIGENT_TIERING")

    # When
    source_url.transition_to_storage_tier("STANDARD")


def test_transition_from_onezone(s3_test_bucket, s3_test_file):
    # Given
    source_url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file}')
    source_url.transition_to_storage_tier("ONEZONE_IA")

    # When
    source_url.transition_to_storage_tier("STANDARD")


def test_transition_to_storage_tier(s3_test_bucket, s3_test_file):
    # Given
    source_url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file}')

    # When
    source_url.transition_to_storage_tier("STANDARD_IA")

    # Then
    assert_that(source_url.object.storage_class).is_equal_to("STANDARD_IA")


def test_restore_from_glacier(s3_test_bucket, s3_test_file):
    # Given
    source_url = S3Url(f's3://{s3_test_bucket.name}/{s3_test_file}')
    source_url.transition_to_storage_tier("GLACIER")

    # When
    source_url.restore_to_storage_tier(1)

    def assert_object_restored():
        obj = source_url.object
        assert_that(obj.restore).is_not_none().starts_with('ongoing-request="false"')

    assert_with_timeout(assert_object_restored, 100, 1)
    assert_that(source_url.object.storage_class).is_equal_to("GLACIER")
    assert_that(source_url.object.restore).is_not_none().starts_with('ongoing-request="false"')

    # When
    source_url.restore_to_storage_tier(1, "Expedited")

    assert_with_timeout(assert_object_restored, 100, 1)
    assert_that(source_url.object.storage_class).is_equal_to("GLACIER")


def test_use_as_dict_key(s3_test_bucket):
    key1 = S3Url(f's3://{s3_test_bucket.name}/test1')
    key2 = S3Url(f's3://{s3_test_bucket.name}/test2')
    key3 = S3Url(f's3://{s3_test_bucket.name}/test3')

    test_dict = {key1: 'value1', key2: 'value2', key3: 'value3'}


    assert_that(test_dict[key1]).is_equal_to('value1')
    assert_that(test_dict[key2]).is_equal_to('value2')
    assert_that(test_dict[key3]).is_equal_to('value3')
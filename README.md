## Overview
S3Url is a helper class that can help with simple operations on AWS S3 objects.<br>
### Installation
`pip install s3-url-helper`<br>
### Usage
    from s3_url import S3Url

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
    file_content: str = file_url.read_text()
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

    # see tests for more examples

## Development notes
init deps:
    
    install dev dependencies: pip install -e ".[dev]" 

build/upload:

    py -m build
    py -m twine upload --repository pypi dist/*  

todo - write docstrings
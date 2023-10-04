from io import IOBase
import json
import threading
from pathlib import Path
from typing import Union, Iterable, IO
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError


class S3Url():
    _local = threading.local()

    def __init__(self, url: Union[str, 'S3Url']):
        '''
        Creates object instance
        :param url: "s3://"-shaped url or another S3Url object
        '''

        if not hasattr(self._local, 's3_res'):
            self._local.s3_res = boto3.resource('s3')
        if isinstance(url, S3Url):
            url = url.url
        else:
            if not url.startswith('s3://'):
                raise Exception(f'Unsupported URL: {url}. It must start with s3://')
        self._parsed = urlparse(url, allow_fragments=False)
        self._object = self._local.s3_res.Object(self.bucket, self.key)

    @classmethod
    def from_url(cls, url: Union[str, 'S3Url']) -> 'S3Url':
        return S3Url(url)

    @classmethod
    def from_bucket_key(cls, bucket: str, key: str) -> 'S3Url':
        return S3Url(f's3://{bucket}/{key}')

    def __repr__(self) -> str:
        return self.url

    def __eq__(self, o: object) -> bool:
        return isinstance(o, S3Url) and o.url == self.url

    def __hash__(self) -> int:
        return self.url.__hash__()

    @property
    def bucket(self) -> str:
        return self._parsed.netloc

    @property
    def key(self) -> str:
        return self._parsed.path.lstrip('/')

    @property
    def object(self):
        return self._object

    @property
    def url(self) -> str:
        return self._parsed.geturl()

    def exists(self) -> bool:
        try:
            self.object.load()
            return True
        except ClientError as cerr:
            if '404' in str(cerr):
                return False
            else:
                raise cerr

    def prefix_exists(self) -> bool:
        try:
            next(iter(self._object.Bucket().objects.filter(Prefix=self.key)))
            return True
        except StopIteration:
            return False
        except ClientError as cerr:
            if '404' in str(cerr):
                return False
            else:
                raise cerr

    def read_text(self, encoding="utf-8-sig") -> str:
        return self.read().decode(encoding)

    def read(self) -> bytes:
        return self.object.get()['Body'].read()

    def read_json(self, encoding="utf-8-sig") -> json:
        return json.loads(self.read_text(encoding))

    def delete(self) -> None:
        self.object.delete()

    def write(self, body: Union[str, bytes], encryption=None) -> None:
        if encryption:
            self.object.put(Body=body, ServerSideEncryption=encryption)
        else:
            self.object.put(Body=body)

    def write_text(self, body: str, encryption=None) -> None:
        self.write(body, encryption)

    def write_json(self, body: json, encryption=None) -> None:
        self.write(json.dumps(body), encryption)

    def upload_file(self, fileobj: IO, encryption=None):
        if encryption:
            self.object.upload_fileobj(fileobj, ExtraArgs={
                'ServerSideEncryption': encryption
            })
        else:
            self.object.upload_fileobj(fileobj)

    def delete_dir(self):
        for obj in self._local.s3_res.Bucket(self.bucket).objects.filter(Prefix=self.key):
            obj.delete()

    def write_tags(self, tags: dict) -> None:
        if tags:
            tag_set = [{'Key': k, 'Value': v} for k, v in tags.items()]
            self._local.s3_res.meta.client.put_object_tagging(
                Bucket=self.bucket,
                Key=self.key,
                Tagging={
                    'TagSet': tag_set
                }
            )

    def read_tags(self) -> dict:
        tags_dict = {}
        tags = self._local.s3_res.meta.client.get_object_tagging(
            Bucket=self.bucket,
            Key=self.key,
        )
        if tags:
            tags_dict = {x['Key']: x['Value'] for x in tags['TagSet']}

        return tags_dict

    def transition_to_storage_tier(self, storage_tier: str):
        return self._local.s3_res.meta.client.copy_object(
            CopySource={
                'Bucket': self.bucket,
                'Key': self.key
            },
            Bucket=self.bucket,
            Key=self.key,
            StorageClass=storage_tier,
            MetadataDirective='COPY')

    def restore_to_storage_tier(self, days: int, retrieval_tier: str = "Standard"):
        return self._local.s3_res.meta.client.restore_object(
            Bucket=self.bucket,
            Key=self.key,
            RestoreRequest={'Days': days, 'GlacierJobParameters': {'Tier': retrieval_tier}})

    def copy_to(self, target_url: Union[str, 'S3Url']) -> None:
        if isinstance(target_url, S3Url):
            target_obj = target_url
        else:
            target_obj = S3Url(target_url)
        self._local.s3_res.meta.client.copy({
            'Bucket': self.bucket,
            'Key': self.key
        }, target_obj.bucket, target_obj.key)

    def copy_from(self, source_url: Union[str, 'S3Url']) -> None:
        if isinstance(source_url, S3Url):
            source_obj = source_url
        else:
            source_obj = S3Url(source_url)
        self._local.s3_res.meta.client.copy({
            'Bucket': source_obj.bucket,
            'Key': source_obj.key
        }, self.bucket, self.key)

    def copy_tags_to(self, target_url: Union[str, 'S3Url']) -> None:
        source_tags = self.read_tags()
        if isinstance(target_url, S3Url):
            target_obj = target_url
        else:
            target_obj = S3Url(target_url)
        target_obj.write_tags(source_tags)

    def copy_tags_from(self, source_url: Union[str, 'S3Url']) -> None:
        if isinstance(source_url, S3Url):
            source_obj = source_url
        else:
            source_obj = S3Url(source_url)
        source_tags = source_obj.read_tags()
        self.write_tags(source_tags)

    def list_prefix_objects(self) -> Iterable['S3Url']:
        for s3_obj in self._object.Bucket().objects.filter(Prefix=self.key):
            yield S3Url(f's3://{s3_obj.bucket_name}/{s3_obj.key}')

    def list_common_prefixes(self) -> Iterable['S3Url']:
        for prefix in self._local.s3_res.meta.client.list_objects(Bucket=self.bucket, Prefix=self.key, Delimiter='/')[
            'CommonPrefixes']:
            yield S3Url(f's3://{self.bucket}/{prefix["Prefix"]}')

    def generate_presigned_url_get(self, timeout=3600) -> str:
        return self._local.s3_res.meta.client.generate_presigned_url(
            ClientMethod='get_object',
            Params={'Bucket': self.bucket, 'Key': self.key},
            ExpiresIn=timeout
        )

    def generate_presigned_url_put(self, timeout=3600) -> str:
        return self._local.s3_res.meta.client.generate_presigned_url(
            ClientMethod='put_object',
            Params={'Bucket': self.bucket, 'Key': self.key},
            ExpiresIn=timeout
        )

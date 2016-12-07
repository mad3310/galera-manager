# -*- coding: utf-8 -*-
from __future__ import absolute_import

import boto3
import urlparse

from tornado.options import options

from src.api.utils.randbytes import randbytes2


class S3(object):

    '''S3 file Storage

    Attributes:
        bucket_name: 存储空间
        prefix_url:  存储空间URL前缀
    '''

    def __init__(self, bucket_name, prefix_url):
        self.bucket_name = bucket_name
        self.prefix_url = prefix_url

    def __repr__(self):
        return '<S3 bucket: %s>' % self.bucket_name

    def _make_auth(self):
        return boto3.resource(service_name='s3',
                              endpoint_url=self.prefix_url,
                              aws_access_key_id=options.S3_ACCESS_KEY,
                              aws_secret_access_key=options.S3_SECRET_KEY)

    @property
    def bucket(self):
        s3 = self._make_auth()
        return s3.Bucket(self.bucket_name)

    def bucket_acl(self, acl):
        """bucket权限控制

        Args:
            acl: 权限控制
        """
        return self.bucket.Acl().put(ACL=acl)

    def object_acl(self, key, acl):
        """文件权限控制

        Args:
            key:  文件名
            acl:  权限控制
        """
        object_acl = self.bucket.Object(key).Acl()
        return object_acl.put(ACL=acl)

    def upload_file(self, file, key=None, acl=None, config=None):
        """上传文件

        Args:
            file: 文件对象
            key:  文件名
            acl: 'private'|'public-read'|'public-read-write'|'authenticated-read'
        """
        key = key or randbytes2()
        self.bucket.upload_file(file, key, Config=config)
        acl = acl or 'public-read'
        self.object_acl(key, acl)
        return key

    def delete_file(self, key):
        """文件删除, 谨慎使用"""
        return self.bucket.Object(key).delete()

    def get_url(self, key):
        """文件下载url"""
        url = ''
        if self.prefix_url:
            url = urlparse.urljoin(self.prefix_url, '/' + self.bucket_name + '/' + key.rstrip('/'))
        else:
            url = ('http://s3.lecloud.com/%s/' % self.bucket) + key
        return url

    def is_file_exists(self, key):
        """文件是否存在"""
        objs = list(self.bucket.objects.filter(Prefix=key))
        return len(objs) > 0 and objs[0].key == key

s3 = S3(options.S3_BUCKET_NAME, options.S3_PREFIX_URL)

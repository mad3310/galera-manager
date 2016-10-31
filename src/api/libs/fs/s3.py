# -*- coding: utf-8 -*-
from __future__ import absolute_import

import boto3

from tornado.options import options

from utils.randbytes2 import randbytes2


class S3(object):
    '''
    S3 file upload system
    Attributes:
        bucket_name: 存储空间
        prefix_urls: 存储空间URL前缀
    '''

    def __init__(self, bucket_name, prefix_url):
        self.bucket_name = bucket_name
        self.prefix_url = prefix_url

    def __repr__(self):
        return '<S3 %s>' % self.bucket_name

    def _make_auth(self):
        return boto3.resource()

    def _token(self, key, expires=3600):
        key = key or randbytes2(16)
        auth = self._make_auth(self.bucket_name, key=key, expires=expires)
        return auth

    def upload_file(self, file, mime_type=None, key=None):
        """
        断点续传上传文件
        Args:
            file:      文件对象
            key:       文件名
        """

    def get_url(self, key):
        """
        文件下载URL
        """

    def delete_file(self, key):
        """
        文件删除, 请安全使用
        """


def make_fs(bucket, prefix_url):
    if hasattr(options, 'QINIU_AK') and options.QINIU_AK:
        return S3(bucket, prefix_url)


fs = make_fs(options.S3_BUCKET_NAME, options.S3_PREFIX_URL)

# -*- coding: utf-8 -*-

from __future__ import absolute_import

import boto3


class S3(object):
    '''
    乐视云存储
    Attributes:
        bucket_name: 存储空间
        prefix_urls: 存储空间URL前缀
        policy:      存储空间上传策略
    '''

    def __init__(self, bucket_name, prefix_urls):
        self.bucket_name = bucket_name
        self.prefix_urls = prefix_urls

    def __repr__(self):
        return '<S3 %s>' % self.bucket_name

    def _make_auth(self):
        return boto3.resource()

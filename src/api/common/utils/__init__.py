# -*- coding: utf-8 -*-

import os
import random
import string

from tornado.options import options

from common.configFileOpers import ConfigFileOpers

confOpers = ConfigFileOpers()


def get_random_password():
    a = list(string.letters + string.digits)
    random.shuffle(a)
    random.shuffle(a)
    random.shuffle(a)
    return "".join(a[:8])


def local_get_zk_address():
    ret_dict = confOpers.getValue(options.zk_address, ['zkAddress', 'zkPort'])
    zk_address = ret_dict['zkAddress']
    zk_port = ret_dict['zkPort']

    if "" == zk_port:
        zk_port = "2181"

    return zk_address, zk_port


def cluster_name():
    """集群名称"""
    cluster_name = ''
    with open('/etc/hostname', 'r') as f:
        res_str = f.readline().replace('d-mcl-', '')
        cluster_name = res_str[0:res_str.find('-n-')]
    return cluster_name

CLUSTER_NAME = cluster_name()


def disk_capacity(path):
    """磁盘总容量：字节"""
    vfs = os.statvfs(path)
    disk_capacity = vfs.f_blocks * vfs.f_bsize
    return disk_capacity


def disk_available(path):
    """磁盘可用容量: 字节"""
    vfs = os.statvfs(path)
    disk_available = vfs.f_bavail * vfs.f_bsize
    return disk_available


def dir_size(dir):
    """文件夹大小：字节"""
    path = os.path
    size = 0L
    for root, dirs, files in os.walk(dir):
        size += sum([path.getsize(path.join(root, name)) for name in files])
    return size

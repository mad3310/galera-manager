# -*- coding: utf-8 -*-

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
    cluster_name = ''
    with open('/etc/hostname', 'r') as f:
        res_str = f.readline().replace('d-mcl-', '')
        cluster_name = res_str[0:res_str.find('-n-')]
    return cluster_name

CLUSTER_NAME = cluster_name()

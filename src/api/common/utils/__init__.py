#-*- coding: utf-8 -*-

import random
import string 

from tornado.options import options
from common.configFileOpers import ConfigFileOpers


def get_random_password():
    a = list(string.letters+string.digits)
    random.shuffle(a)
    random.shuffle(a)
    random.shuffle(a)
    return "".join(a[:8])


def local_get_zk_address():
    confOpers = ConfigFileOpers()
    ret_dict = confOpers.getValue(options.zk_address, ['zkAddress','zkPort'])
    zk_address_local = ret_dict['zkAddress']
    zk_port_local = ret_dict['zkPort']
    del confOpers
    return zk_address_local, zk_port_local
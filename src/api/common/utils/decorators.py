#!/usr/bin/env python 2.6.6
#coding:utf-8
import logging
from kazoo.handlers.threading import TimeoutError
from common.utils import local_get_zk_address
from common.utils.exceptions import CommonException


def singleton(cls):
    
    instances = {}
    
    def _singleton(*args, **kw):

        zk_addr, zk_port = local_get_zk_address()
        if not (zk_addr and zk_port):
            raise CommonException('zookeeper address and port are not written!')
        
        if cls not in instances:
            logging.info('init class : %s' % str(cls))
            instances[cls] = cls(*args, **kw)
        return instances[cls]
    return _singleton


def timeout_handler(func):
    def _timeout_handler(*args, **kwargs):
        try:
            ret = func(*args, **kwargs)
            return ret
        except TimeoutError, e:
            logging.info('func : %s called timeout, please check zookeeper service status' % func.__name__)
            pass
    return _timeout_handler
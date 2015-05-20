#!/usr/bin/env python 2.6.6
#coding:utf-8
import logging
from kazoo.handlers.threading import TimeoutError


def singleton(cls):
    
    instances = {}
    
    def _singleton(*args, **kw):
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
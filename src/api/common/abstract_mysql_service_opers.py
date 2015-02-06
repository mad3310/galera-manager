#-*- coding: utf-8 -*-
import base64
import logging
import threading
import sched
import time
import urllib
import tornado

from tornado.options import options
from tornado.httpclient import AsyncHTTPClient, HTTPClient
from tornado.httpclient import HTTPRequest
from tornado.gen import Wait, Callback, engine
from abc import ABCMeta, abstractmethod
from common.zkOpers import ZkOpers
from common.helper import  get_zk_address

'''
Created on 2013-7-21

@author: asus
'''

class Abstract_Mysql_Service_Opers(object):
    
#    zkOper = ZkOpers('127.0.0.1',2181)
    
    @abstractmethod
    def start(self):
        raise NotImplementedError, "Cannot call abstract method"
    
    @abstractmethod
    def stop(self):
        raise NotImplementedError, "Cannot call abstract method"
    

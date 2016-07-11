'''
Created on 01.07, 2016

@author: xu
'''
import logging
import json
import tornado.httpclient
from tornado.gen import Callback, Wait, engine
from tornado.options import options
from tornado.httpclient import HTTPRequest
from common.helper import _retrieve_userName_passwd
import urllib, urllib2


class BaseBackupWorker(object):
    
    def __init__(self):
        '''
        Constructor
        '''
        self.response_message = None


    def _dispatch_request_sync(self, online_node_list, get_or_post_type, url_post):
        http_client = tornado.httpclient.HTTPClient()

        adminUser, adminPasswd = _retrieve_userName_passwd()
        try:
            response_message = {}
            for node_ip in online_node_list:
                requesturi = "http://" + node_ip + ":"+ str(options.port) + url_post
                request = HTTPRequest(url=requesturi, method=get_or_post_type, auth_username=adminUser, auth_password=adminPasswd)
                response = http_client.fetch(request)
                response_message.setdefault(node_ip, json.loads(response.body.strip()))
            return response_message
        finally:
            http_client.close()
    
    
    @engine
    def _dispatch_request(self, online_node_list, get_or_post_type, url_post, params={}):
        adminUser, adminPasswd = _retrieve_userName_passwd()
        params = urllib.urlencode(params)
        
        http_client = tornado.httpclient.AsyncHTTPClient()
        key_sets = set()
        try:
            for node_ip in online_node_list:
                requesturi = "http://"+ node_ip +":"+str(options.port)+ url_post
                callback_key = str(node_ip)
                key_sets.add(callback_key)
                request = HTTPRequest(url=requesturi, method=get_or_post_type, body=params, auth_username=adminUser, auth_password=adminPasswd)
                http_client.fetch(request, callback=(yield Callback(callback_key)))

            response_message = {}
            for i in range(len(online_node_list)):
                callback_key = key_sets.pop()
                response = yield Wait(callback_key)
                if response.error:
                    continue
                else:
                    response_message.setdefault(callback_key, json.loads(response.body.strip()))
            logging.info(response_message)

        finally:
            http_client.close()

    
    @property
    def _get_response_message(self):
        return self.response_message
    
    def _set_response_message(self, val=None):
        self.response_message = val
        
    def _analysis_usable_backup_node(self, system_loads, free_spaces, free_memory):
        backupevlscore = Retrive_Node_Score()
        _class = backupevlscore._retrive_node_info()
        
        host_ip = _class._analysis_usable_backup_node(system_loads, free_spaces, free_memory)
        return host_ip


class Retrive_Node_Score():
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''
    def _retrive_node_info(self):
  
        #module_path = 'backup_evl_score'
        #cls_name = 'BackupEvlScore'
        #module_obj = importlib.import_module(module_path)
        #score_class = getattr(module_obj, cls_name)()
        try:
            from backup_utils.backup_evl_score import BackupEvlScore
        except ImportError:
            logging.info('no BackupEvlScore module')
        
        score_class = BackupEvlScore()
        
        return score_class      

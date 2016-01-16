'''
Created on 01.07, 2016

@author: xu
'''
import logging
import json
import tornado.httpclient
from tornado.gen import Callback, Wait
from tornado.options import options
from tornado.httpclient import HTTPRequest
from common.helper import _retrieve_userName_passwd
import importlib
import urllib


class BaseBackupWorker(object):
    
    def __init__(self):
        '''
        Constructor
        '''
 
    def _dispatch_request(self, online_node_list, get_or_post_type, url_post, params={}):
        adminUser, adminPasswd = _retrieve_userName_passwd()
        http_client = tornado.httpclient.AsyncHTTPClient()
        params = urllib.urlencode(params)
        key_sets = set()
        try:
            for node_ip in online_node_list:
                requesturi = "http://"+ node_ip +":"+str(options.port)+ url_post
                callback_key = str(node_ip)
                key_sets.add(callback_key)
                request = HTTPRequest(url=requesturi, method=get_or_post_type, auth_username=adminUser, auth_password=adminPasswd)
                http_client.fetch(request, callback=(yield Callback(callback_key)), body=params)
            
            response_message = {}

            for i in range(len(online_node_list)):
                callback_key = key_sets.pop()
                response = yield Wait(callback_key)
                if response.error:
                    continue
                else:
                    logging.info("response: %s" % str(response.body.strip()))
                    response_message.setdefault(callback_key, json.loads(response.body.strip()))
        finally:
            http_client.close()
            
        return response_message
        
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
  
        module_path = 'backup_evl_score'
        cls_name = 'BackupEvlScore'
        
        module_obj = importlib.import_module(module_path)
        score_class = getattr(module_obj, cls_name)()
        
        return score_class      

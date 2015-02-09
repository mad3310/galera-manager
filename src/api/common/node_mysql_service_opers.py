#-*- coding: utf-8 -*-
import base64
import logging
import threading
import sched
import time
import urllib
import tornado
import sys

from common.invokeCommand import InvokeCommand
from common.helper import get_zk_address
from tornado.options import options
from tornado.httpclient import AsyncHTTPClient, HTTPClient
from tornado.httpclient import HTTPRequest
from tornado.gen import Wait, Callback, engine
from common.dba_opers import DBAOpers
from common.utils.mail import send_email
from common.configFileOpers import ConfigFileOpers
from abc import ABCMeta, abstractmethod
from common.zkOpers import ZkOpers
from common.abstract_mysql_service_opers import Abstract_Mysql_Service_Opers
from common.utils.threading_exception_queue import Threading_Exception_Queue
from common.abstract_mysql_service_action_thread import Abstract_Mysql_Service_Action_Thread

'''
Created on 2013-7-21

@author: asus
'''

class Node_Mysql_Service_Opers(Abstract_Mysql_Service_Opers):
    
    invokeCommand = InvokeCommand()
    
    def __init__(self):
        self.zkOper = None
        '''
        Constructor
        '''
        
    def retrieve_recover_position(self):
        result = self.invokeCommand.run_check_shell(options.retrieve_node_uuid_seqno_script)
        uuid = self.__find_special_value(result, "uuid:", 37)
        seqno = self.__find_special_value(result, "seqno:", 65535)
        
        dict = {}
        dict.setdefault("uuid", uuid)
        dict.setdefault("seqno", seqno)        
        
        return dict
    
    def __find_special_value(self, result, key, value_length):
        key_start_pos = result.find(key)
        key_end_pos = key_start_pos+len(key)
        value = result[key_end_pos:key_end_pos+value_length]
        value = value.rstrip('\n')
        return value
        
        
    def start(self, isNewCluster):

        zk_address = get_zk_address()
        zkoper_obj = ZkOpers(zk_address, 2181)
        self.zkOper = zkoper_obj
        isLock,lock = self.zkOper.lock_node_start_stop_action()
        
        node_start_action = Node_start_action(isNewCluster, lock)
        node_start_action.start()
        
    def stop(self):
        
        zk_address = get_zk_address()
        zkoper_obj = ZkOpers(zk_address, 2181)
        self.zkOper = zkoper_obj
        isLock,lock = self.zkOper.lock_node_start_stop_action()
        
        # Start a thread to run the events
        node_stop_action = Node_stop_action(lock)
        node_stop_action.start()
    
class Node_start_action(Abstract_Mysql_Service_Action_Thread):
    lock = None
    isNewCluster = False
    
    dba_opers = DBAOpers()
    
    def __init__(self, isNewCluster, lock):
        super(Node_start_action, self).__init__()
        self.lock = lock
        self.isNewCluster = isNewCluster
        
    def run(self):
        try:
            self._issue_start_action(self.isNewCluster, self.lock)
        except:
            self.threading_exception_queue.put(sys.exc_info())
        
    def _issue_start_action(self, isNewCluster, lock):
        dataNodeProKeyValue = self.confOpers.getValue(options.data_node_property, ['dataNodeIp'])
        data_node_ip = dataNodeProKeyValue['dataNodeIp']
        
        try:
            finished_flag = self.dba_opers.retrieve_wsrep_status()
            
            if not finished_flag:
                result = self.invokeCommand.remove_mysql_socket()
                result = self.invokeCommand.mysql_service_start(isNewCluster)
            
                finished_flag = self._check_start_status(data_node_ip)
        finally:

            zk_address = get_zk_address()
            zkoper_obj = ZkOpers(zk_address, 2181)
            zkoper_obj.unLock_node_start_stop_action(lock)
            zkoper_obj.close()
        
        if finished_flag:    
            self._send_email(data_node_ip, " mysql service start operation finished")
            
   
    def _check_start_status(self, data_node_ip):
        finished_flag = False
        
        sh_name = "ps -ef | grep mysqld_safe | grep -iv grep | wc -l"
        count = 10
        while not finished_flag and count >= 0:
            
            result = self.invokeCommand.run_check_shell(sh_name)
            
            if int(result) == 0:
                finished_flag = False
                count -= 1
                #break
            
            finished_flag = self.dba_opers.retrieve_wsrep_status()
                        
            time.sleep(2)

        
        if finished_flag:   
            zk_address = get_zk_address()
            zkoper_obj = ZkOpers(zk_address, 2181)
            zkoper_obj.write_started_node(data_node_ip)
            zkoper_obj.close()
        return finished_flag
            
        
class Node_stop_action(Abstract_Mysql_Service_Action_Thread):
    lock = None
    
    def __init__(self, lock):
        super(Node_stop_action, self).__init__()
        self.lock = lock
        
    def run(self):
        try:
            self._issue_stop_action(self.lock)
        except:
            self.threading_exception_queue.put(sys.exc_info())
        
    def _issue_stop_action(self, lock):
        finished_flag = False
        
        dataNodeProKeyValue = self.confOpers.getValue(options.data_node_property, ['dataNodeIp'])
        data_node_ip = dataNodeProKeyValue['dataNodeIp']
        
        try:
            result = self.invokeCommand.mysql_service_stop()
            finished_flag = self._check_stop_status(data_node_ip)
        finally:

            zk_address = get_zk_address()
            zkoper_obj = ZkOpers(zk_address, 2181)
            zkoper_obj.unLock_node_start_stop_action(lock)
            zkoper_obj.close()
        if finished_flag:    
            self._send_email(data_node_ip, " mysql service stop operation finished")
        
        
    def _check_stop_status(self, data_node_ip):
        sh_name = "ps -ef | grep mysqld_safe | grep -iv grep | wc -l"
        
        finished_flag = False
        retry_count = 0
        
        while not finished_flag and retry_count <= 60:
            result = self.invokeCommand.run_check_shell(sh_name)
            
            if int(result) == 0:
                finished_flag = True
                
            retry_count = retry_count + 1
                
            time.sleep(2)
        
        if finished_flag:
            zk_address = get_zk_address()
            zkoper_obj = ZkOpers(zk_address, 2181)
            self.zkOper.remove_started_node(data_node_ip)
            zkoper_obj.close()
        return finished_flag
            

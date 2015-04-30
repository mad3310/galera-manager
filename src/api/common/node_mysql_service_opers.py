#-*- coding: utf-8 -*-
import time
import sys

from common.invokeCommand import InvokeCommand
from tornado.options import options
from common.dba_opers import DBAOpers
from common.zkOpers import ZkOpers
from common.abstract_mysql_service_opers import Abstract_Mysql_Service_Opers
from common.abstract_mysql_service_action_thread import Abstract_Mysql_Service_Action_Thread
from common.utils.exceptions import CommonException
from common.configFileOpers import ConfigFileOpers
from common.utils.mail import send_email
from common.helper import get_localhost_ip

'''
Created on 2013-7-21

@author: asus
'''

class Node_Mysql_Service_Opers(Abstract_Mysql_Service_Opers):
    
    invokeCommand = InvokeCommand()
    
    confFileOper = ConfigFileOpers()
    
    def __init__(self):
        '''
        Constructor
        '''
        
    def retrieve_recover_position(self):
        result = self.invokeCommand.run_check_shell(options.retrieve_node_uuid_seqno_script)
        uuid = self.__find_special_value(result, "uuid:", 37)
        seqno = self.__find_special_value(result, "seqno:", 65535)
        
        result = {}
        result.setdefault("uuid", uuid)
        result.setdefault("seqno", seqno)
        
        return dict
    
    def __find_special_value(self, result, key, value_length):
        key_start_pos = result.find(key)
        key_end_pos = key_start_pos+len(key)
        value = result[key_end_pos:key_end_pos+value_length]
        value = value.rstrip('\n')
        return value
        
        
    def start(self, isNewCluster):
        zkOper = ZkOpers()
        
        '''
        @todo: check only pass lock to below action and close the zkOper object, real action if can release the lock
        '''
        try:
            isLock,lock = zkOper.lock_node_start_stop_action()
            
            if not isLock:
                raise CommonException("When start node, can't retrieve the start atcion lock!")
                
            node_start_action = Node_start_action(isNewCluster, lock)
            node_start_action.start()
        finally:
            zkOper.stop()
         
    def stop(self):
        zkOper = ZkOpers()
        
        try:
            isLock,lock = zkOper.lock_node_start_stop_action()
            
            if not isLock:
                raise CommonException("When stop node, can't retrieve the stop action lock!")
            
            # Start a thread to run the events
            node_stop_action = Node_stop_action(lock)
            node_stop_action.start()
        finally:
            zkOper.stop()
            
    def retrieve_log_for_error(self):
        result = self.invokeCommand.run_check_shell(options.check_datanode_error)
        
        if cmp('false',result) == 0:
            _tmp_error_log_file_path = '%s/shell/tmp_check_datanode_error' % (options.base_dir)
            _mysql_error_log_message = self.confFileOper.retrieveFullText(_tmp_error_log_file_path)
            _email_subject = "[%s] MySQL log error message" % options.sitename
            self._send_log_info_email(_email_subject, _mysql_error_log_message)
            
        return result
    
    def retrieve_log_for_warning(self):
        result = self.invokeCommand.run_check_shell(options.check_datanode_warning)
        
        if cmp('false',result) == 0:
            _tmp_warning_log_file_path = '%s/shell/tmp_check_datanode_warning' % (options.base_dir)
            _mysql_error_log_message = self.confFileOper.retrieveFullText(_tmp_warning_log_file_path)
            _email_subject = "[%s] MySQL log warning message" % options.sitename
            self._send_log_info_email(_email_subject, _mysql_error_log_message)
            
        return result
    
    def _send_log_info_email(self, subject, content):
        local_ip = get_localhost_ip()
        # send email
        body = self.render_string("errors/500_email.html",
                                  exception=content)
        
        body += "\nip:" + local_ip
        
        if options.send_email_switch:
            send_email(options.admins, subject, body)
        
        
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
        
        zkOper = ZkOpers()
        try:
            finished_flag = self.dba_opers.retrieve_wsrep_status()
            
            if not finished_flag:
                self.invokeCommand.remove_mysql_socket()
                self.invokeCommand.mysql_service_start(isNewCluster)
            
                finished_flag = self._check_start_status(data_node_ip)
        finally:
            zkOper.unLock_node_start_stop_action(lock)
            zkOper.stop()
        
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
            zkoper = ZkOpers()
            try:
                zkoper.write_started_node(data_node_ip)
            finally:
                zkoper.close()
            
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
        
        zkoper = ZkOpers()
        try:
            self.invokeCommand.mysql_service_stop()
            finished_flag = self._check_stop_status(data_node_ip)
        finally:
            zkoper.unLock_node_start_stop_action(lock)
            zkoper.close()
            
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
            zkOper = ZkOpers()
            try:
                zkOper.remove_started_node(data_node_ip)
            finally:
                zkOper.stop()
            
            
        return finished_flag
    


            

#!/usr/bin/env python
#-*- coding: utf-8 -*-

'''
Created on 2013-7-11

@author: asus
'''
import json
import threading
import logging

from tornado.options import options
from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import SessionExpiredError
from kazoo.handlers.threading import TimeoutError
from kazoo.retry import KazooRetry
from common.utils.exceptions import CommonException
from common.my_logging import debug_log
from common.utils.decorators import singleton, timeout_handler
from common.utils import local_get_zk_address
from common.configFileOpers import ConfigFileOpers
from common.helper import getDictFromText

log_obj = debug_log('zkOpers')
logger = log_obj.get_logger_object()

confOpers = ConfigFileOpers()
        

class ZkOpers(object):
    
    zk = None
    
    DEFAULT_RETRY_POLICY = KazooRetry(
        max_tries=None,
        max_delay=10000,
    )
    
    rootPath = "/letv/mysql/mcluster"
    
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        self.zkaddress, self.zkport = local_get_zk_address()
        if "" != self.zkaddress and "" != self.zkport:
            self.zk = KazooClient(
                                  hosts=self.zkaddress+':'+str(self.zkport), 
                                  connection_retry=self.DEFAULT_RETRY_POLICY,
                                  timeout=20)
            self.zk.add_listener(self.listener)
            self.zk.start()
            #self.zk = self.ensureinstance()
            logging.info("instance zk client (%s:%s)" % (self.zkaddress, self.zkport))

    def getclustername(self):
        try:
            f = open('/etc/hostname','r')
            res_str = f.readline().replace('d-mcl-','')
            return res_str[0:res_str.find('-n-')]
        except Exception, e:
            raise 'hostname is wrong! please check it %s' %f.readline()
        finally:
            f.close()

    def command(self, cmd):
        return self.zk.command(cmd)

    def watch(self):
        clusterUUID = self.getClusterUUID()
        myConfPath = self.rootPath + "/" + clusterUUID + "/mycnf"
        @self.zk.DataWatch(myConfPath)
        def watch_my_conf(data, stat):
            keyList = ["wsrep_cluster_address"]
            dic = getDictFromText(data, keyList)
            confOpers.setValue(options.mysql_cnf_file_name, dic)

    def close(self):
        try:
            self.zk.stop()
            self.zk.close()
        except Exception, e:
            logging.error(e)
            
    def stop(self):
        try:
            self.zk.stop()
        except Exception, e:
            logging.error(e)
            raise

    def listener(self, state):
        if state == KazooState.LOST:
            logging.info("zk connect lost, stop this connection and then start new one!")
            
        elif state == KazooState.SUSPENDED:
            logging.info("zk connect suspended, stop this connection and then start new one!")
            self.re_connect()
        else:
            pass
            
    def is_connected(self):
        return self.zk.state == KazooState.CONNECTED

    def re_connect(self):
        zk = KazooClient(hosts=self.zkaddress+':'+str(self.zkport), connection_retry=self.DEFAULT_RETRY_POLICY)
        zk.start()
        self.zk = zk
        return self.zk

    def reset_zk_client(self, count=0):

        while count < 5:
            try:
                return self.re_connect()
            
            except SessionExpiredError, e:
                logging.info("zk client retry time: %s, for zookeeper service may stop" % (count))
                return self.reset_zk_client(count + 1)
            
            except TimeoutError, e:
                logging.info("zk client retry time: %s, for connect timeout" % (count))
                return self.reset_zk_client(count + 1)
        
        raise TimeoutError, "zookeeper connection timeout"

    def ensureinstance(self, count=0):
        if self.is_connected():
            return self.zk
        else:
            return self.reset_zk_client(count)
    
    @timeout_handler
    def existCluster(self):
        self.DEFAULT_RETRY_POLICY(self.zk.ensure_path, self.rootPath)
        path = self.rootPath + '/' + self.getclustername()
        if self.zk.exists(path):
            return True
        return False
    
    @timeout_handler
    def getDataNodeNumber(self,clusterUUID):
        path = self.rootPath + "/" + clusterUUID + "/dataNode"
        dataNodeNumber = self.DEFAULT_RETRY_POLICY(self.zk.get_children, path)
        return dataNodeNumber
    
    @timeout_handler
    def getClusterUUID(self):
        try: 
            dataNodeName = self.DEFAULT_RETRY_POLICY(self.zk.get_children, self.rootPath+'/'+self.getclustername())
        except SessionExpiredError:
            dataNodeName = self.DEFAULT_RETRY_POLICY(self.zk.get_children, self.rootPath+'/'+self.getclustername())
            
        if dataNodeName is None or dataNodeName.__len__() == 0:
            raise CommonException('cluster uuid is null.please check the zk connection or check if existed cluster uuid.')
        
        return self.getclustername() +'/'+ dataNodeName[0]
        

    def writeClusterInfo(self,clusterUUID,clusterProps):
        path = self.rootPath + "/" + clusterUUID
        self.zk.ensure_path(path)
        self.DEFAULT_RETRY_POLICY(self.zk.set, path, str(clusterProps))#vesion need to write
        
    def writeClusterStatus(self, clusterProps):
        clusterUUID = self.getClusterUUID()
        path = self.rootPath + "/" + clusterUUID + "/cluster_status"
        self.zk.ensure_path(path)
        self.DEFAULT_RETRY_POLICY(self.zk.set, path, str(clusterProps))  
        
    def retrieveClusterStatus(self):
        #self.zk = self.ensureinstance()
        clusterUUID = self.getClusterUUID()
        path = self.rootPath + "/" + clusterUUID + "/cluster_status"
        resultValue = self._retrieveSpecialPathProp(path)
        return resultValue   
    
    def writeDataNodeInfo(self,clusterUUID,dataNodeProps):
        dataNodeIp = dataNodeProps['dataNodeIp']
        path = self.rootPath + "/" + clusterUUID + "/dataNode/" + dataNodeIp
        self.zk.ensure_path(path)
        self.DEFAULT_RETRY_POLICY(self.zk.set, path, str(dataNodeProps))#version need to write
        
    def retrieve_data_node_list(self):
        clusterUUID = self.getClusterUUID()
        path = self.rootPath + "/" + clusterUUID + "/dataNode"
        data_node_ip_list = self._return_children_to_list(path)
        return data_node_ip_list
    
    def retrieve_data_node_info(self, ip_address):
        clusterUUID = self.getClusterUUID()
        path = self.rootPath + "/" + clusterUUID + "/dataNode/" + ip_address
        resultValue = self._retrieveSpecialPathProp(path)
        return resultValue
    
    @timeout_handler
    def writeMysqlCnf(self,clusterUUID,mysqlCnfPropsFullText):
        path = self.rootPath + "/" + clusterUUID + "/mycnf"
        self.zk.ensure_path(path)
        self.DEFAULT_RETRY_POLICY(self.zk.set, path, mysqlCnfPropsFullText)#version need to write
        #self.DEFAULT_RETRY_POLICY(self.zk.get, path, func)
        
    def write_db_info(self, clusterUUID, dbName, dbProps):
        path = self.rootPath + "/" + clusterUUID + "/db/" + dbName 
        self.zk.ensure_path(path)
        self.DEFAULT_RETRY_POLICY(self.zk.set, path, str(dbProps))#version need to write
    
    def retrieve_db_list(self):
        clusterUUID = self.getClusterUUID()
        path = self.rootPath + "/" + clusterUUID + "/db"
        db_list = self._return_children_to_list(path)
        return  db_list
    
    def retrieve_db_user_list(self, dbName):
        clusterUUID = self.getClusterUUID()
        path = self.rootPath + "/" + clusterUUID + "/db/" + dbName
        db_user_list = self._return_children_to_list(path)
        return db_user_list
   
    def get_db_user_prop(self, dbName, dbUser):
        clusterUUID = self.getClusterUUID()
        path  = self.rootPath + "/" + clusterUUID + "/db/" + dbName + "/" + dbUser
        resultValue = self._retrieveSpecialPathProp(path)
        return resultValue

    def write_db_backup_info(self, dict_status):
        clusterUUID = self.getClusterUUID()
        path = self.rootPath + "/" + clusterUUID + "/monitor_status/" + "db/" + "backup"
        self.zk.ensure_path(path)
        self.DEFAULT_RETRY_POLICY(self.zk.set, path, str(dict_status))

    def write_user_info(self,clusterUUID,dbName,username,ipAddress,userProps):
        path = self.rootPath + "/" + clusterUUID + "/db/" + dbName + "/" + username + "|" + ipAddress
        self.zk.ensure_path(path)
        self.DEFAULT_RETRY_POLICY(self.zk.set, path, str(userProps))#version need to write
        
    @timeout_handler  
    def retrieveClusterProp(self,clusterUUID):
        resultValue = {}
        path = self.rootPath + "/" + clusterUUID
        if self.zk.exists(path):
            resultValue = self.DEFAULT_RETRY_POLICY(self.zk.get, path)
            
        return resultValue
    
    @timeout_handler    
    def retrieveMysqlProp(self,clusterUUID,func=None):
        resultValue = {}
        path = self.rootPath + "/" + clusterUUID + "/mycnf"
        if self.zk.exists(path):
            resultValue = self.DEFAULT_RETRY_POLICY(self.zk.get, path,func)
            
        return resultValue
    
    def retrieve_db_prop(self,clusterUUID,dbName):
        path = self.rootPath + "/" + clusterUUID + "/db/" + dbName
        resultValue = self._retrieveSpecialPathProp(path)
        return resultValue
    
    @timeout_handler
    def retrieve_db_user_prop(self,clusterUUID,dbName):
        path = self.rootPath + "/" + clusterUUID + "/db/" + dbName
        self.zk.ensure_path(path)
        user_ipAddress_list = self.DEFAULT_RETRY_POLICY(self.zk.get_children, path)
        
        user_ipAddress_list_return = {}
        if len(user_ipAddress_list) != 0:
            for i in range(len(user_ipAddress_list)):
                user_ipAddress_item = user_ipAddress_list[i]
                user_ipAddress_seq = user_ipAddress_item.split('|')
                user_ipAddress_list_return.setdefault(user_ipAddress_seq[0],user_ipAddress_seq[1])
        return user_ipAddress_list_return
    
    def remove_db(self, clusterUUID, dbName):
        path = self.rootPath + "/" + clusterUUID + "/db/" + dbName
        if self.zk.exists(path):
            self.DEFAULT_RETRY_POLICY(self.zk.delete, path)
    
    def remove_data_node_name(self, data_node_ip):
        clusterUUID = self.getClusterUUID()
        path = self.rootPath + "/" + clusterUUID + "/dataNode/" + data_node_ip
        if self.zk.exists(path):
            self.DEFAULT_RETRY_POLICY(self.zk.delete, path)
            
    def remove_db_user(self, clusterUUID, dbName, userName, ipAddress):
        path = self.rootPath + "/" + clusterUUID + "/db/" + dbName + "/" + userName + "|" + ipAddress
        if self.zk.exists(path):
            self.DEFAULT_RETRY_POLICY(self.zk.delete, path)
            
    def retrieve_user_limit_props(self, clusterUUID, dbName, userName, ipAddress):
        path = self.rootPath + "/" + clusterUUID + "/db/" + dbName + "/" + userName + "|" + ipAddress
        resultValue = self._retrieveSpecialPathProp(path)
        return resultValue
            
#     def check_concurrent_initing(self):
#         clusterUUID = self.getClusterUUID()
#         path = self.rootPath + "/" + clusterUUID + "/init_data_nodes"
#         logging.info("check children:" + path)
#         self.zk.ensure_path(path)
#         dataNodeIps = self.zk.get_children(path)
#         if len(dataNodeIps) != 0:
#             return True
#         return False
    
#     def write_concurrent_init_data_node(self,data_node_ip):
#         clusterUUID = self.getClusterUUID()
#         path = self.rootPath + "/" + clusterUUID + "/init_data_nodes/" + data_node_ip
#         logging.info("create data node:" + path)
#         self.zk.ensure_path(path)
        
#     def remove_concurrent_init_data_node(self,data_node_ip):
#         clusterUUID = self.getClusterUUID()
#         path = self.rootPath + "/" + clusterUUID + "/init_data_nodes/" + data_node_ip
#         logging.info("remove data node:" + path)
#         if self.zk.exists(path):
#             self.zk.delete(path)
            
    def write_started_node(self, data_node_ip):
        clusterUUID = self.getClusterUUID()
        path = self.rootPath + "/" + clusterUUID + "/monitor_status/node/started/" + data_node_ip
        self.zk.ensure_path(path)
        
    def remove_started_node(self, data_node_ip):
        clusterUUID = self.getClusterUUID()
        path = self.rootPath + "/" + clusterUUID + "/monitor_status/node/started/" + data_node_ip
        if self.zk.exists(path):
            self.DEFAULT_RETRY_POLICY(self.zk.delete, path)
            
    def retrieve_started_nodes(self):
        #self.zk = self.ensureinstance()
        clusterUUID = self.getClusterUUID()
        path = self.rootPath + "/" + clusterUUID + "/monitor_status/node/started"
        started_nodes = self._return_children_to_list(path)
        return started_nodes
            
    def write_monitor_status(self, monitor_type, monitor_key, monitor_value):
        clusterUUID = self.getClusterUUID()
        path = self.rootPath + "/" + clusterUUID + "/monitor_status/" + monitor_type +"/"+ monitor_key
        self.zk.ensure_path(path)
        self.DEFAULT_RETRY_POLICY(self.zk.set, path, str(monitor_value))#version need to write
 
    def retrieve_monitor_status_list(self, monitor_type):
        clusterUUID = self.getClusterUUID()
        path = self.rootPath + "/" + clusterUUID + "/monitor_status/" + monitor_type
        monitor_status_type_list = self._return_children_to_list(path)
        return monitor_status_type_list
      
    def retrieve_monitor_status_value(self, monitor_type, monitor_key):
        clusterUUID = self.getClusterUUID()
        path = self.rootPath + "/" + clusterUUID + "/monitor_status/" + monitor_type + "/" + monitor_key
        resultValue = self._retrieveSpecialPathProp(path)
        return resultValue
    
    def retrieve_monitor_type(self):
        clusterUUID = self.getClusterUUID()
        path = self.rootPath + "/" + clusterUUID + "/monitor_status"
        monitor_type_list = self._return_children_to_list(path)
        return monitor_type_list
    
#     def election_monitor_master(self, data_node_ip, func):
#         clusterUUID = self.getClusterUUID()
#         path = self.rootPath + "/" + clusterUUID + "/election/monitor"
#         election = self.zk.Election(path, data_node_ip)
#         # blocks until the election is won, then calls monitor async method
#         election.run(func)
        
    def lock_cluster_start_stop_action(self):
        lock_name = "cluster_start_stop"
        return self._lock_base_action(lock_name)
    
    def unLock_cluster_start_stop_action(self, lock):
        self._unLock_base_action(lock)
            
    def lock_node_start_stop_action(self):
        lock_name = "node_start_stop"
        return self._lock_base_action(lock_name)
    
    def unLock_node_start_stop_action(self, lock):
        self._unLock_base_action(lock)
        
    def lock_async_monitor_action(self):
        lock_name = "async_monitor"
        return self._lock_base_action(lock_name)
    
    def unLock_aysnc_monitor_action(self, lock):
        self._unLock_base_action(lock)
        
    def lock_init_node_action(self):
        lock_name = "init_node"
        return self._lock_base_action(lock_name)
        
    def unLock_init_node_action(self, lock):
        self._unLock_base_action(lock)
            
    def _lock_base_action(self, lock_name):
        clusterUUID = self.getClusterUUID()
        path = "%s/%s/lock/%s" % (self.rootPath, clusterUUID, lock_name) 
        lock = self.DEFAULT_RETRY_POLICY(self.zk.Lock, path, threading.current_thread())
        isLock = lock.acquire(blocking=True, timeout=5)
        return (isLock,lock)
        
    def _unLock_base_action(self, lock):
        if lock is not None:
            lock.release()
    
    @timeout_handler
    def _return_children_to_list(self, path):
        self.zk.ensure_path(path)
        children = self.DEFAULT_RETRY_POLICY(self.zk.get_children, path)
        
        children_to_list = []
        if len(children) != 0:
            for i in range(len(children)):
                children_to_list.append(children[i])
        return children_to_list
    
    @timeout_handler
    def _retrieveSpecialPathProp(self,path):
        data = None
        
        if self.zk.exists(path):
            data,_ = self.DEFAULT_RETRY_POLICY(self.zk.get, path)
        
        resultValue = {}
        if data != None and data != '':
            resultValue = self._format_data(data)
            
        return resultValue
    
    def _format_data(self, data):
        local_data = data.replace("'", "\"").replace("[u\"", "[\"").replace(" u\"", " \"")
        formatted_data = json.loads(local_data)
        return formatted_data


@singleton
class Scheduler_ZkOpers(ZkOpers):
    
    def __init__(self):
        '''
        Constructor
        '''
        ZkOpers.__init__(self)

@singleton
class Watch_ZkOpers(ZkOpers):
    
    def __init__(self):
        '''
        Constructor
        '''
        ZkOpers.__init__(self)
        self.watch()

@singleton
class Requests_ZkOpers(ZkOpers):
    
    def __init__(self):
        '''
        Constructor
        '''
        ZkOpers.__init__(self)
        


@singleton
class Abstract_ZkOpers(ZkOpers):
    
    def __init__(self):
        '''
        Constructor
        '''
        ZkOpers.__init__(self) 


@singleton
class Mysql_Thread_ZkOpers(ZkOpers):
    
    def __init__(self):
        '''
        Constructor
        '''
        ZkOpers.__init__(self)     

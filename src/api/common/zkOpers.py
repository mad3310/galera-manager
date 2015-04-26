#!/usr/bin/env python
#-*- coding: utf-8 -*-

'''
Created on 2013-7-11

@author: asus
'''
import json

from tornado.options import options
from common.configFileOpers import ConfigFileOpers
from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import SessionExpiredError
from kazoo.handlers.threading import TimeoutError
from kazoo.retry import KazooRetry
import logging
import threading
from common.utils.exceptions import CommonException

class ZkOpers(object):
    
    rootPath = "/letv/mysql/mcluster"
    
    confOpers = ConfigFileOpers()
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        self.zkaddress, self.zkport = self.local_get_zk_address()
        
        self.zk = KazooClient(
            hosts=self.zkaddress+':'+str(self.zkport),
            connection_retry = KazooRetry(delay=1, max_tries=5, max_delay=30)
        )
        self.zk.start()
        self.zk.add_listener(self.listener)
        
    def listener(self, state):
        if state == KazooState.LOST:
            self.zk.start()
        elif state == KazooState.SUSPENDED:
            print "*******listener saw KazooState.LOST"
        else:
            print "*******listener saw KazooState.CONNECT"

    def local_get_zk_address(self):
        ret_dict = self.confOpers.getValue(options.zk_address, ['zkAddress','zkPort'])
        zk_address = ret_dict['zkAddress']
        zk_port = ret_dict['zkPort']
        return zk_address, zk_port

    def close(self):
        try:
            self.zk.stop()
            self.zk.close()
        except Exception, e:
            logging.error(e)

        
    def existCluster(self):
        self.zk.ensure_path(self.rootPath)
        clusters = self.zk.get_children(self.rootPath)
        if len(clusters) != 0:
            return True
        return False
    
    def getDataNodeNumber(self,clusterUUID):
        path = self.rootPath + "/" + clusterUUID
        dataNodeNumber = self.zk.get_children(path)
        return dataNodeNumber
    
    def getClusterUUID(self):
        try: 
            dataNodeName = self.zk.get_children(self.rootPath)
        except SessionExpiredError:
            dataNodeName = self.zk.get_children(self.rootPath)
            
        if dataNodeName is None or dataNodeName.__len__() == 0:
            raise CommonException('cluster uuid is null.please check the zk connection or check if existed cluster uuid.')
        
        return dataNodeName[0]
        
        
    def writeClusterInfo(self,clusterUUID,clusterProps):
        path = self.rootPath + "/" + clusterUUID
        self.zk.ensure_path(path)
        self.zk.set(path, str(clusterProps))#vesion need to write
        
    def writeClusterStatus(self, clusterProps):
        clusterUUID = self.getClusterUUID()
        path = self.rootPath + "/" + clusterUUID + "/cluster_status"
        self.zk.ensure_path(path)
        self.zk.set(path, str(clusterProps))  
        
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
        self.zk.set(path, str(dataNodeProps))#version need to write
        
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
        
    def writeMysqlCnf(self,clusterUUID,mysqlCnfPropsFullText,func):
        path = self.rootPath + "/" + clusterUUID + "/mycnf"
        self.zk.ensure_path(path)
        self.zk.set(path, mysqlCnfPropsFullText)#version need to write
        self.zk.get(path, func)
        
    def write_db_info(self,clusterUUID,dbName,dbProps):
        path = self.rootPath + "/" + clusterUUID + "/db/" + dbName 
        self.zk.ensure_path(path)
        self.zk.set(path, str(dbProps))#version need to write
    
    def retrieve_db_list(self):
        clusterUUID = self.getClusterUUID()
        logging.info("ClusterUUID:" + clusterUUID)
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
        self.zk.set(path, str(dict_status))

    def write_user_info(self,clusterUUID,dbName,username,ipAddress,userProps):
        path = self.rootPath + "/" + clusterUUID + "/db/" + dbName + "/" + username + "|" + ipAddress
        self.zk.ensure_path(path)
        self.zk.set(path, str(userProps))#version need to write
        
        
    def retrieveClusterProp(self,clusterUUID):
        resultValue = {}
        path = self.rootPath + "/" + clusterUUID
        if self.zk.exists(path):
            resultValue = self.zk.get(path)
            
        return resultValue
        
    def retrieveMysqlProp(self,clusterUUID,func=None):
        resultValue = {}
        path = self.rootPath + "/" + clusterUUID + "/mycnf"
        if self.zk.exists(path):
            resultValue = self.zk.get(path,func)
            
        return resultValue
    
    def retrieve_db_prop(self,clusterUUID,dbName):
        path = self.rootPath + "/" + clusterUUID + "/db/" + dbName
        resultValue = self._retrieveSpecialPathProp(path)
        return resultValue
    
    def retrieve_db_user_prop(self,clusterUUID,dbName):
        path = self.rootPath + "/" + clusterUUID + "/db/" + dbName
        self.zk.ensure_path(path)
        user_ipAddress_list = self.zk.get_children(path)
        
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
            self.zk.delete(path)
            
    def remove_db_user(self, clusterUUID, dbName, userName, ipAddress):
        path = self.rootPath + "/" + clusterUUID + "/db/" + dbName + "/" + userName + "|" + ipAddress
        if self.zk.exists(path):
            self.zk.delete(path)
            
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
            self.zk.delete(path)
            
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
        self.zk.set(path, str(monitor_value))#version need to write
        
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
        lock = self.zk.Lock(path, threading.current_thread())
        isLock = lock.acquire(True,1)
        return (isLock,lock)
        
    def _unLock_base_action(self, lock):
        if lock is not None:
            lock.release()
    
    
    def _return_children_to_list(self, path):
        self.zk.ensure_path(path)
        children = self.zk.get_children(path)
        
        children_to_list = []
        if len(children) != 0:
            for i in range(len(children)):
                children_to_list.append(children[i])
        return children_to_list
    
    def _retrieveSpecialPathProp(self,path):
        data = None
        
        if self.zk.exists(path):
            data,stat = self.zk.get(path)
        
        resultValue = {}
        if data != None and data != '':
            resultValue = self._format_data(data)
            
        return resultValue
    
    def _format_data(self, data):
        local_data = data.replace("'", "\"").replace("[u\"", "[\"").replace(" u\"", " \"")
        formatted_data = json.loads(local_data)
        return formatted_data
        
if __name__ == "__main__":
    try:
        zkOpers = ZkOpers('127.0.0.1', 2181)
        path = "/letv/mysql/mcluster/"
        # Print the version of a node and its data
        data, stat = zkOpers.ensureinstance().get(path)
        #data, stat = zkOpers.zk.get(path)
        print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))

    # List the children
        while True:
            children = zkOpers.ensureinstance().get_children(path)
            #children = zkOpers.zk.get_children(path)
            print("There are %s children with names %s" % (len(children), children))
    except TimeoutError, e:
        print e

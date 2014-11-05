#!/usr/bin/env python
#-*- coding: utf-8 -*-

'''
Created on 2013-7-11

@author: asus
'''
from kazoo.client import KazooClient
from kazoo.exceptions import SessionExpiredError

import logging
import threading
from common.my_logging import debug_log
class ZkOpers(object):
    
    zk = None
    
    rootPath = "/letv/mysql/mcluster"
    log_obj = debug_log('root')
    logger = log_obj.get_logger_object()
    
    '''
    classdocs
    '''
    def __init__(self,zkAddress,zkPort):
        '''
        Constructor
        '''
#         self.log_obj = debug_log('root')
#         self.logger = self.log_obj.get_logger_object()
        self.zkaddress = zkAddress
        self.zkport = zkPort
        self.zk = self.instance(self.zkaddress, self.zkport)
        #self.zk = KazooClient(hosts=self.zkaddress+':'+str(self.zkport))
        #self.zk.start()
        
    def my_listener(self, stat):
        if stat == KazooClient.LOST:
            self.zk = self.instance(self.zkaddress, self.zkport)
        elif stat == KazooClient.SUSPENDED:
            self.zk = self.instance(self.zkaddress, self.zkport)
        else:
            pass
            
    def instance(self, address, port):
        self.__zkc = KazooClient(hosts=address+":"+str(port))
        self.__zkc.add_listener(self.my_listener)
        self.__zkc.start()
        logging.info("instance zk client (%s:%s)" % (address, port))
        return self.__zkc
        
    def ensureinstance(self):
        count = 0
        while count < 10:
            try:
                clusters = self.zk.get_children(self.rootPath)
                if len(clusters) != 0:
                    return self.zk
            except SessionExpiredError, e:
                logging.error("zk client retry time: %s" % (count))
                self.zk = KazooClient(hosts=self.zkaddress+':'+str(self.zkport))
                self.zk.start()
            finally:
                count += 1
        
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
        self.logger.debug(self.rootPath)
        dataNodeName = self.zk.get_children(self.rootPath)
        self.logger.debug(dataNodeName)
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
        self.zk = self.ensureinstance()
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
        self.logger.debug("the started data node:" + data_node_ip)
        self.zk.ensure_path(path)
        
    def remove_started_node(self, data_node_ip):
        clusterUUID = self.getClusterUUID()
        path = self.rootPath + "/" + clusterUUID + "/monitor_status/node/started/" + data_node_ip
        self.logger.debug("the removed data node:" + data_node_ip)
        if self.zk.exists(path):
            self.zk.delete(path)
            
    def retrieve_started_nodes(self):
        self.zk = self.ensureinstance()
        clusterUUID = self.getClusterUUID()
        path = self.rootPath + "/" + clusterUUID + "/monitor_status/node/started"
        started_nodes = self._return_children_to_list(path)
        return started_nodes
            
    def write_monitor_status(self, monitor_type, monitor_key, monitor_value):
        clusterUUID = self.getClusterUUID()
        path = self.rootPath + "/" + clusterUUID + "/monitor_status/" + monitor_type +"/"+ monitor_key
        self.logger.debug("monitor status:" + path)
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
        self.logger.debug("check children:" + path)
        self.zk.ensure_path(path)
        children = self.zk.get_children(path)
        
        children_to_list = []
        if len(children) != 0:
            for i in range(len(children)):
                children_to_list.append(children[i])
        return children_to_list
    
    def _retrieveSpecialPathProp(self,path):
        self.logger.debug(path)
        
        data = None
        
        if self.zk.exists(path):
            self.logger.debug(path+" existed")
            data,stat = self.zk.get(path)
            
        self.logger.debug(data)
        
        resultValue = {}
        if data != None and data != '':
            resultValue = eval(data)
            
        return resultValue
    
        
if __name__ == "__main__":
    zkOpers = ZkOpers()
    path = "/letv/mysql/mcluster/"
    # Print the version of a node and its data
    data, stat = zkOpers.zk.get(path)
    print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))

    # List the children
    children = zkOpers.zk.get_children(path)
    print("There are %s children with names %s" % (len(children), children))
        
#-*- coding: utf-8 -*-

'''
Created on 2013-7-21

@author: asus
'''
from base import APIHandler
import json
# retrieve the status value of special monitor type, the monitor type include cluster,node,db.
# In different monitor type, there are many of monitor points. 
# eg. curl "http://localhost:8888/mcluster/status/{cluster,node,db}"         
class MclusterStatusDetail(APIHandler):
   
    def get(self, monitor_type):
        if monitor_type == None:
            raise "monitor type should be not null!"
        zkoper_obj = ZkOpers()
        self.zkOper = zkoper_obj
        monitor_status_list = self.zkOper.retrieve_monitor_status_list(monitor_type)
        monitor_status_list_count = len(monitor_status_list)
        
        dict = {}
        for i in range(monitor_status_list_count):
            monitor_status_key = monitor_status_list[i]
            monitor_status_value = self.zkOper.retrieve_monitor_status_value(monitor_type, monitor_status_key)
            dict.setdefault(monitor_status_key, monitor_status_value)
        
        self.finish(dict)
        self.zkOper.close()
# retrieve the status value of all monitor type 
# eg. curl "http://localhost:8888/mcluster/status"          
class MclusterStatus(APIHandler):
 
    def get(self):
        zkoper_obj = ZkOpers()
        self.zkOper = zkoper_obj
        
        monitor_types = self.zkOper.retrieve_monitor_type()
        dict = {}
        for i in range(len(monitor_types)):
            monitor_type = monitor_types[i]
            monitor_status_list = self.zkOper.retrieve_monitor_status_list(monitor_type)
            monitor_status_list_count = len(monitor_status_list)
            
            monitor_type_sub_dict = {}
            for i in range(monitor_status_list_count):
                monitor_status_key = monitor_status_list[i]
                monitor_status_value = self.zkOper.retrieve_monitor_status_value(monitor_type, monitor_status_key)
                monitor_type_sub_dict.setdefault(monitor_status_key, monitor_status_value)
                
            dict.setdefault(monitor_type,monitor_type_sub_dict)
        
        self.finish(dict)
        self.zkOper.close()

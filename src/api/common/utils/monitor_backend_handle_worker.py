import logging
import kazoo
import threading

from handlers.monitor import Cluster_Info_Async_Handler, Node_Info_Async_Handler, DB_Info_Async_Handler
from common.zkOpers import ZkOpers
from common.utils.exceptions import CommonException

class Monitor_Backend_Handle_Worker(threading.Thread):
    
    cluster_handler = Cluster_Info_Async_Handler()
  
    node_handler = Node_Info_Async_Handler()
    
    db_handler = DB_Info_Async_Handler()

    def __init__(self):
        super(Monitor_Backend_Handle_Worker,self).__init__()
        
        self.zkOper = ZkOpers()
        
        try:
            self.isLock, lock = self.zkOper.lock_async_monitor_action()
        except kazoo.exceptions.LockTimeout:
            logging.info("a thread is running the monitor async, give up this oper on this machine!")
            return
            
        if not self.isLock:
            raise CommonException('a thread is running the monitor async, give up this oper on this machine!')
        
        self.lock = lock
        
    def run(self):
        try:
            data_node_info_list = self.zkOper.retrieve_data_node_list()
            self.__action_monitor_async(data_node_info_list)
        finally:
            if self.lock:
                self.zkOper.unLock_aysnc_monitor_action(self.lock)
            if self.zkOper:
                self.zkOper.stop()
        
                
    def __action_monitor_async(self, data_node_info_list):
        cluster_status_dict =  self.cluster_handler.retrieve_info(data_node_info_list)
        node_status_dict = self.node_handler.retrieve_info(data_node_info_list)
        db_status_dict = self.db_handler.retrieve_info(data_node_info_list)

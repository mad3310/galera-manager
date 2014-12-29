import logging
import kazoo
import threading

from tornado.ioloop import PeriodicCallback
from handlers.monitor import Cluster_Info_Async_Handler, Node_Info_Async_Handler, DB_Info_Async_Handler
from common.zkOpers import ZkOpers
from common.helper import check_leader
class Monitor_Backend_Handle_Worker(threading.Thread):
    
    cluster_handler = Cluster_Info_Async_Handler()
  
   
    node_handler = Node_Info_Async_Handler()
    
    db_handler = DB_Info_Async_Handler()
    

    zkOper = ZkOpers('127.0.0.1',2181)
    
    def __init__(self):
        super(Monitor_Backend_Handle_Worker,self).__init__()
            
            
    def run(self):
        
        leader_flag = check_leader()
        if leader_flag == False:
            logging.info('This node is not leader of zookeeper!')
            return
        logging.info("This node is leader of zookeeper.")
        isLock = False
        lock = None
        
        try:
            isLock,lock = self.zkOper.lock_async_monitor_action()
        except kazoo.exceptions.LockTimeout:
            logging.info("a thread is running the monitor async, give up this oper on this machine!")
        
        if isLock:
            try:
                self.__action_monitor_async()
            except Exception, e:
                logging.error(e)
            finally:
                self.zkOper.unLock_aysnc_monitor_action(lock)
                
                
    def __action_monitor_async(self):
        data_node_info_list = self.zkOper.retrieve_data_node_list()
        cluster_status_dict =  self.cluster_handler.retrieve_info(data_node_info_list)
        node_status_dict = self.node_handler.retrieve_info(data_node_info_list)
        db_status_dict = self.db_handler.retrieve_info(data_node_info_list)

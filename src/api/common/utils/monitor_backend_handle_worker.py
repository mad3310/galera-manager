import logging
import kazoo
import threading
from tornado.options import options
from common.configFileOpers import ConfigFileOpers
from tornado.ioloop import PeriodicCallback
from handlers.monitor import Cluster_Info_Async_Handler, Node_Info_Async_Handler, DB_Info_Async_Handler
from common.zkOpers import ZkOpers
from common.helper import check_leader, get_zk_address
from common.invokeCommand import InvokeCommand
class Monitor_Backend_Handle_Worker(threading.Thread):
    
    cluster_handler = Cluster_Info_Async_Handler()
  
    node_handler = Node_Info_Async_Handler()
    
    db_handler = DB_Info_Async_Handler()

#    zkOper = ZkOpers('127.0.0.1',2181)
    confOpers = ConfigFileOpers()
    invokeCommand = InvokeCommand()

    def __init__(self):
        self.zkOper = None
        super(Monitor_Backend_Handle_Worker,self).__init__()
            
            
    def run(self):
        
        zk_address = get_zk_address()
        logging.info("zk address " + str(zk_address))
        if zk_address == "": 
            logging.info("zk address is none") 
            return
        else:
            if check_leader() == False: 
                return 
        logging.info("This node is leader of zookeeper.")
        isLock = False
        lock = None

        zkoper_obj = ZkOpers()
        self.zkOper = zkoper_obj
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
                self.zkOper.close()
                logging.info("close zk client connection successfully")
                
    def __action_monitor_async(self):
        data_node_info_list = self.zkOper.retrieve_data_node_list()
        cluster_status_dict =  self.cluster_handler.retrieve_info(data_node_info_list)
        node_status_dict = self.node_handler.retrieve_info(data_node_info_list)
        db_status_dict = self.db_handler.retrieve_info(data_node_info_list)

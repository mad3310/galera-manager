import logging
import kazoo
import threading

from common.configFileOpers import ConfigFileOpers
from handlers.monitor import Cluster_Info_Async_Handler, Node_Info_Async_Handler, DB_Info_Async_Handler
from common.zkOpers import ZkOpers
from common.utils.exceptions import CommonException
from common.helper import get_zk_address
from common.invokeCommand import InvokeCommand

class Monitor_Backend_Handle_Worker(threading.Thread):
    
    cluster_handler = Cluster_Info_Async_Handler()
  
    node_handler = Node_Info_Async_Handler()
    
    db_handler = DB_Info_Async_Handler()

    confOpers = ConfigFileOpers()
    invokeCommand = InvokeCommand()

    def __init__(self):
        super(Monitor_Backend_Handle_Worker,self).__init__()
            
            
    def run(self):
        '''
        @todo: need to check the zk lead method, when use outside zk cluster
        '''
        zk_address = get_zk_address()
        if "" == zk_address: 
            raise CommonException('zk address is none!')
        
        isLock = False
        lock = None

        zkOper = ZkOpers()
        try:
            isLock,lock = zkOper.lock_async_monitor_action()
            
            if not isLock:
                raise CommonException('This node is not leader of zookeeper!')
            
            data_node_info_list = zkOper.retrieve_data_node_list()
            self.__action_monitor_async(data_node_info_list)
        except kazoo.exceptions.LockTimeout:
            logging.info("a thread is running the monitor async, give up this oper on this machine!")
        finally:
            zkOper.unLock_aysnc_monitor_action(lock)
            zkOper.close()
        
                
    def __action_monitor_async(self, data_node_info_list):
        cluster_status_dict =  self.cluster_handler.retrieve_info(data_node_info_list)
        node_status_dict = self.node_handler.retrieve_info(data_node_info_list)
        db_status_dict = self.db_handler.retrieve_info(data_node_info_list)

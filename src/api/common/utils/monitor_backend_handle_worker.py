import logging
import kazoo

from handlers.monitor import Cluster_Info_Async_Handler, Node_Info_Async_Handler, DB_Info_Async_Handler
from common.zkOpers import Scheduler_ZkOpers
from common.utils.exceptions import CommonException
from common.helper import check_leader
from common.utils import local_get_zk_address


class Monitor_Backend_Handle_Worker(object):
    
    cluster_handler = Cluster_Info_Async_Handler()
  
    node_handler = Node_Info_Async_Handler()
    
    db_handler = DB_Info_Async_Handler()

    def __init__(self):
        super(Monitor_Backend_Handle_Worker,self).__init__()
        
    def run(self):
        
        leader_flag = check_leader()
        if leader_flag == False:
            logging.info("This node is not the leader of zookeeper, give up this chance")
            return
        
        logging.info("This node is leader of zookeeper.")
        
        try:
            '''
                if no logic below, singleton Scheduler_ZkOpers may have no self.zk object.
            '''
            
            zk_addr, zk_port = local_get_zk_address()
            if not (zk_addr and zk_port):
                return
            
            zkOper = Scheduler_ZkOpers()
            logging.info('check zk is connected :%s' % str(zkOper.is_connected()) )
            isLock, lock = zkOper.lock_async_monitor_action()
        except kazoo.exceptions.LockTimeout:
            logging.info("a thread is running the monitor async, give up this oper on this machine!")
            return
            
        if not isLock:
            raise CommonException('a thread is running the monitor async, give up this oper on this machine!')
        
        try:
            data_node_info_list = zkOper.retrieve_data_node_list()
            self.__action_monitor_async(data_node_info_list)
        finally:
            if lock is not None:
                zkOper.unLock_aysnc_monitor_action(lock)

    def __action_monitor_async(self, data_node_info_list):
        cluster_status_dict =  self.cluster_handler.retrieve_info(data_node_info_list)
        node_status_dict = self.node_handler.retrieve_info(data_node_info_list)
        db_status_dict = self.db_handler.retrieve_info(data_node_info_list)

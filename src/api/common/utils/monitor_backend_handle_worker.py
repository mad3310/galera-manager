import logging
import kazoo

from handlers.monitor import Node_Info_Async_Handler, DB_Info_Async_Handler
from common.zkOpers import Scheduler_ZkOpers
from common.utils.exceptions import CommonException
from common.helper import check_leader
from common.utils import local_get_zk_address


class Monitor_Backend_Handle_Worker(object):
  
    node_handler = Node_Info_Async_Handler()
    
    db_handler = DB_Info_Async_Handler()

    def __init__(self):
        super(Monitor_Backend_Handle_Worker,self).__init__()
        
    def run(self):
        '''
            if no logic below, singleton Scheduler_ZkOpers may have no self.zk object.
        '''
        zk_addr, zk_port = local_get_zk_address()
        if not (zk_addr and zk_port):
            return
            
        zkOper = Scheduler_ZkOpers()
        logging.info('check zk is connected :%s' % str(zkOper.is_connected()))
        leader_flag = check_leader(zkOper)
        if leader_flag == False:
            logging.info("This node is not the leader of zookeeper, give up this chance")
            return
            
        logging.info("This node is leader of zookeeper.")
            
        try:
            isLock, lock = None, None
            isLock, lock = zkOper.lock_async_monitor_action()
            if not isLock:
                raise CommonException('a thread is running the monitor async, give up this oper on this machine!')
            
            data_node_info_list = zkOper.retrieve_data_node_list()
            self.__action_monitor_async(data_node_info_list)
            logging.info('this monitoring is over~')
            
        except kazoo.exceptions.LockTimeout:
            logging.info("a thread is running the monitor async, give up this oper on this machine!")
            return
        
        finally:
            if lock is not None:
                zkOper.unLock_aysnc_monitor_action(lock)

    def __action_monitor_async(self, data_node_info_list):
        node_status_dict = self.node_handler.retrieve_info(data_node_info_list)
        db_status_dict = self.db_handler.retrieve_info(data_node_info_list)

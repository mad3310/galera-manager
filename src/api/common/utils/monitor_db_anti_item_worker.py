import logging
import threading

from common.helper import is_monitoring,get_localhost_ip
from common.zkOpers import Scheduler_ZkOpers
from common.helper import check_leader
from common.utils import local_get_zk_address
from common.status_opers import Check_DB_Anti_Item

TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
class Monitor_Db_Anti_Item(threading.Thread):
    
    check_db_anti_itmes = Check_DB_Anti_Item()

    def __init__(self):
        super(Monitor_Db_Anti_Item,self).__init__()
        
    def run(self):    
        '''
            if no logic below, singleton Scheduler_ZkOpers may have no self.zk object.
        '''
        
        zk_addr, zk_port = local_get_zk_address()
        if not (zk_addr and zk_port):
            return
        
        zkOper = Scheduler_ZkOpers()
        leader_flag = check_leader(zkOper)
        if leader_flag == False:
            logging.info("This node is not the leader of zookeeper, give up this chance")
            return
        
        if not is_monitoring(get_localhost_ip(), zkOper):
            return
        logging.info('do db anti item monitor~' )

        data_node_info_list = zkOper.retrieve_data_node_list()
        self.__action_monitor_async(data_node_info_list)
        logging.info('do db anti monitor over~' )

    def __action_monitor_async(self, data_node_info_list):
        self.check_db_anti_itmes.check(data_node_info_list)
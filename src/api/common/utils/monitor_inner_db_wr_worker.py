import logging

from common.helper import is_monitoring,get_localhost_ip
from common.zkOpers import Scheduler_ZkOpers
from common.helper import check_leader
from common.utils import local_get_zk_address
from common.status_opers import Check_DB_WR_Available

TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
class Monitor_Db_Wr_Available(object):
    
    check_db_wr_available = Check_DB_WR_Available()

    def __init__(self):
        super(Monitor_Db_Wr_Available, self).__init__()
        
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
        
        logging.info('do db wr check monitor~' )
        data_node_info_list = zkOper.retrieve_data_node_list()
        self.__action_monitor_async(data_node_info_list)
        logging.info('finish db wr check~' )

    def __action_monitor_async(self, data_node_info_list):
        self.check_db_wr_available.check(data_node_info_list)
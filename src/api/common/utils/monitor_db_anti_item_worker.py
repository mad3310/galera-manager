import logging
import kazoo
from common.helper import is_monitoring,get_localhost_ip
from common.zkOpers import Scheduler_ZkOpers
from common.utils.exceptions import CommonException
from common.helper import check_leader
from common.utils import local_get_zk_address
from common.status_opers import Check_DB_Anti_Item

TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
class Monitor_Db_Anti_Item(object):
    

    def __init__(self):
        super(Monitor_Db_Anti_Item,self).__init__()
        
    def run(self):
        
        leader_flag = check_leader()
        if leader_flag == False:
            logging.info("This node is not the leader of zookeeper, give up this chance")
            return
        
        logging.info("This node is leader of zookeeper.")
        
        '''
            if no logic below, singleton Scheduler_ZkOpers may have no self.zk object.
        '''
        
        zk_addr, zk_port = local_get_zk_address()
        if not (zk_addr and zk_port):
            return
        
        zkOper = Scheduler_ZkOpers()
        if not is_monitoring(get_localhost_ip(), zkOper):
            return
        logging.info('check zk is connected :%s' % str(zkOper.is_connected()) )

        data_node_info_list = zkOper.retrieve_data_node_list()
        self.__action_monitor_async(data_node_info_list)

    def __action_monitor_async(self, data_node_info_list):
        __check_db_anti_itmes = Check_DB_Anti_Item()
        self.__check_db_anti_items.check(data_node_info_list)


import logging
import time
import kazoo
from common.zkOpers import Scheduler_ZkOpers
from common.status_opers import Check_DB_WR_Available
from common.abstract_mysql_service_action_thread import Abstract_Mysql_Service_Action_Thread

class Monitor_Db_Wr_Available(Abstract_Mysql_Service_Action_Thread):
    
    check_db_wr_available = Check_DB_WR_Available()

    def __init__(self, timeout):
        super(Monitor_Db_Wr_Available,self).__init__()
        self.timeout = timeout
        
    def run(self):
        '''
            if no logic below, singleton Scheduler_ZkOpers may have no self.zk object.
        ''' 
        begin_time = time.time()
        
        zkOper = Scheduler_ZkOpers()
        logging.info('check zk is connected :%s' % str(zkOper.is_connected()))

        isLock, lock = None, None
        try:
            isLock, lock = zkOper.lock_async_monitor_wr_action()
            if not isLock:
                return
        except kazoo.exceptions.LockTimeout:
            logging.info("a thread is running the monitor async, give up this oper on this machine!")
            return
        
        try:
            data_node_info_list = zkOper.retrieve_data_node_list()
            self.__action_monitor_async(data_node_info_list)
            logging.info('do db wr monitor over~' )
            end_time = time.time()
            time_span = int(end_time - begin_time)
            
            if time_span < self.timeout-1:
                time.sleep(self.timeout-1 - time_span)

        except Exception, e:
            self.threading_exception_queue.put(e.exc_info())
        
        finally:
            if lock is not None:
                zkOper.unLock_aysnc_monitor_action(lock)

    def __action_monitor_async(self, data_node_info_list):
        self.check_db_wr_available.check(data_node_info_list)

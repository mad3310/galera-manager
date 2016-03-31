import logging
import kazoo
import time
from common.zkOpers import Scheduler_ZkOpers
from common.abstract_mysql_service_action_thread import Abstract_Mysql_Service_Action_Thread
from common.utils.monitor_utils import Monitor_handle_Asyc


class Monitor_Backend_Handle_Worker(Abstract_Mysql_Service_Action_Thread):

    def __init__(self, timeout):
        super(Monitor_Backend_Handle_Worker,self).__init__()
        self.timeout = timeout
        
    def run(self):
        '''
            if no logic below, singleton Scheduler_ZkOpers may have no self.zk object.
        ''' 
        begin_time = time.time()
        lock_name = "async_monitor/async_monitor_handler"
        
        zkOper = Scheduler_ZkOpers()
        logging.info('check zk is connected :%s' % str(zkOper.is_connected()))
    
        isLock, lock = None, None
        try:
            isLock, lock = zkOper.lock_async_monitor_action(lock_name)
            if not isLock:
                return
        except kazoo.exceptions.LockTimeout:
            logging.info("a thread is running the monitor async, give up this oper on this machine!")
            return
        
        try:
            data_node_info_list = zkOper.retrieve_data_node_list()
            self.__monitor_task_asyc(lock_name, data_node_info_list, begin_time)

        except Exception, e:
            self.threading_exception_queue.put(e.exc_info())

        finally:
            if lock is not None:
                zkOper.unLock_aysnc_monitor_action(lock)


    def __monitor_task_asyc(self, lock_name, data_node_info_list, time):
        real_asyc_monitor = Monitor_handle_Asyc()
        real_asyc_monitor._action_monitor_async(lock_name, data_node_info_list, time, timeout = self.timeout)


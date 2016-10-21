# -*- coding: utf-8 -*-

import sys
import logging
import kazoo
import time

from common.zkOpers import Scheduler_ZkOpers
from common.abstract_mysql_service_action_thread import Abstract_Mysql_Service_Action_Thread
from handlers.monitor import Node_Info_Async_Handler, DB_Info_Async_Handler, Check_DB_Anti_Item, Check_DB_WR_Available


class Monitor_Backend_Handle_Worker(Abstract_Mysql_Service_Action_Thread):

    node_handler = Node_Info_Async_Handler()

    db_handler = DB_Info_Async_Handler()

    check_db_anti_itmes = Check_DB_Anti_Item()

    check_db_wr_available = Check_DB_WR_Available()

    def __init__(self, timeout, monitor_type):
        super(Monitor_Backend_Handle_Worker, self).__init__()
        self.timeout = timeout
        self.monitor_type = monitor_type

        '''estimate unlocking zookeeper need time(secord)
        '''
        self.time_constant = 1

    def run(self):
        '''
            if no logic below, singleton Scheduler_ZkOpers may have no self.zk object.
        '''
        begin_time = time.time()
        lock_name = 'async_monitor/' + self.monitor_type

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
            getattr(self, '_async_' + self.monitor_type)(data_node_info_list)

            end_time = time.time()
            monitor_exc_time = int(end_time - begin_time)

            '''leave timeout for sleep
            '''
            real_time_out = self.timeout - self.time_constant

            if monitor_exc_time < real_time_out:
                time.sleep(real_time_out - monitor_exc_time)

            logging.info("%s task has finished" %self.monitor_type)

        except Exception, e:
            self.threading_exception_queue.put(sys.exc_info())

        finally:
            if lock is not None:
                zkOper.unLock_aysnc_monitor_action(lock)

    def _async_monitor(self, data_node_info_list):
        self.node_handler.retrieve_info(data_node_info_list)
        self.db_handler.retrieve_info(data_node_info_list)

    def _async_monitor_anti(self, data_node_info_list):
        self.check_db_anti_itmes.check(data_node_info_list)

    def _async_monitor_write_read(self, data_node_info_list):
        self.check_db_wr_available.check(data_node_info_list)

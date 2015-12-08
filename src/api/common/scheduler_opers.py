#-*- coding: utf-8 -*-
import sys

from tornado.ioloop import PeriodicCallback
from common.utils.threading_exception_handle_worker import Thread_Exception_Handler_Worker
from common.utils.monitor_backend_handle_worker import Monitor_Backend_Handle_Worker
from common.utils.monitor_db_anti_item_worker import Monitor_Db_Anti_Item
from common.utils.threading_exception_queue import Threading_Exception_Queue
from common.utils.monitor_inner_db_wr_worker import Monitor_Db_Wr_Available

'''
Created on 2013-7-21

@author: asus
'''

class Scheduler_Opers(object):
    '''
    classdocs
    '''
    threading_exception_queue = Threading_Exception_Queue()

    def __init__(self):
        '''
        Constructor
        '''
        self.thread_exception_hanlder(5)
        self.sced_monitor_handler(55)
        self.check_db_wr_available(155)
        self.check_db_anti_item(195)
            
        
    def sced_monitor_handler(self, action_timeout = 30):
        # Create a periodic callback that tries to access async monitor interface
        if action_timeout > 0:
            _monitor_async_t = PeriodicCallback(self.__create_worker_check_monitor,
                action_timeout * 1000)
            _monitor_async_t.start()
            
    def __create_worker_check_monitor(self):
        monitor_backend_worker = Monitor_Backend_Handle_Worker()
        try:
            monitor_backend_worker.run()
        except Exception:
            self.threading_exception_queue.put(sys.exc_info())
    
    def check_db_anti_item(self, action_timeout):
        if action_timeout > 0:
            _anti_async_t = PeriodicCallback(self.__check_db_anti, action_timeout * 1000)
            _anti_async_t.start()
    
    def __check_db_anti(self):
        try:
            db_anti_hanlder_worker = Monitor_Db_Anti_Item()
            db_anti_hanlder_worker.start()
        except Exception:
            self.threading_exception_queue.put(sys.exc_info())
            
    def check_db_wr_available(self, action_timeout):
        if action_timeout > 0:
            _monitor_async_t = PeriodicCallback(self.__check_inner_db_wr_available,
            action_timeout * 1000)
        _monitor_async_t.start()
        
    def __check_inner_db_wr_available(self):
        try:
            db_wr_hanlder_worker = Monitor_Db_Wr_Available()
            db_wr_hanlder_worker.start()
        except Exception:
            self.threading_exception_queue.put(sys.exc_info())
            
    def thread_exception_hanlder(self, action_timeout = 5):
        if action_timeout > 0:
            _exception_async_t = PeriodicCallback(self.__create_worker_exception_handler,
                action_timeout * 1000)
            _exception_async_t.start()
            
    def __create_worker_exception_handler(self):
        exception_hanlder_worker = Thread_Exception_Handler_Worker()
        exception_hanlder_worker.start()
        
        

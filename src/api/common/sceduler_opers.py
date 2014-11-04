#-*- coding: utf-8 -*-

from tornado.ioloop import IOLoop, PeriodicCallback
from common.utils.threading_exception_handle_worker import Thread_Exception_Handler_Worker
from common.utils.monitor_backend_handle_worker import Monitor_Backend_Handle_Worker

'''
Created on 2013-7-21

@author: asus
'''

class Sceduler_Opers(object):
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''
        self.thread_exception_hanlder(5)
        self.sced_monitor_handler(50)
        
        
        
    def sced_monitor_handler(self, action_timeout = 30):
        # Create a periodic callback that tries to access async monitor interface
        if action_timeout > 0:
            _monitor_async_t = PeriodicCallback(self.__create_worker_check_monitor,
                action_timeout * 1000)
            _monitor_async_t.start()
            
    def __create_worker_check_monitor(self):
        monitor_backend_worker = Monitor_Backend_Handle_Worker()
        monitor_backend_worker.start()
            
    def thread_exception_hanlder(self, action_timeout = 5):
        if action_timeout > 0:
            _exception_async_t = PeriodicCallback(self.__create_worker_exception_handler,
                action_timeout * 1000)
            _exception_async_t.start()
            
    def __create_worker_exception_handler(self):
        exception_hanlder_worker = Thread_Exception_Handler_Worker()
        exception_hanlder_worker.start()
        
        

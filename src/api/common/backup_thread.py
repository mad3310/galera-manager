#!/usr/bin/env python
import threading
import logging
import datetime

from tornado.options import options
from common.invokeCommand import InvokeCommand
from common.utils.exceptions import HTTPAPIError

TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

class backup_thread(threading.Thread):
    
    def __init__(self, backup_mode):
        self._backup_mode = backup_mode
        threading.Thread.__init__(self)

    def run(self):
        
        invokeCommand = InvokeCommand()
        now = datetime.datetime.now()
        logging.info("Backup start time is " + now.strftime(TIME_FORMAT))
        logging.info(str(self.flag))
        if self._backup_mode == "full":
            ret_sub_p = invokeCommand._runSysCmdnoWait(options.full_back_sh)
            logging.info("ret_sub_p" + str(ret_sub_p))
            if ret_sub_p == False:
                raise HTTPAPIError(status_code=411, error_detail="Full Back up process terminated!",\
                                notification = "direct", \
                                log_message= "Full Back up process terminated!",\
                                response =  "Full Back up process terminated!")
        else:
            ret_sub_p =  invokeCommand._runSysCmdnoWait(options.inc_back_sh)
            if ret_sub_p == False:
                raise HTTPAPIError(status_code=411, error_detail="Increment Back up process terminated!",\
                                notification = "direct", \
                                log_message= "Increment Back up process terminated!",\
                                response =  "Increment Back up process terminated!")
                
        
        logging.info("wait here")
        ret_sub_p.wait()

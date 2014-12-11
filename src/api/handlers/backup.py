#-*- coding: utf-8 -*-

import re
import os
import time
import Queue
import logging
import datetime
import threading
import multiprocessing
import tornado.httpclient

from Queue import Empty
from Queue import Queue

from os import listdir
from os.path import isfile

from base import APIHandler
from tornado.options import options
from tornado.web import asynchronous
from common.invokeCommand import InvokeCommand
from common.utils.exceptions import HTTPAPIError
from handlers.backup_thread import backup_thread



# Start backing up database data.
#eq curl  "http://localhost:8888/backup" backup data by full dose.

queue = multiprocessing.Queue()

class BackUp(APIHandler):
    
#    global store_list
    @asynchronous
    def get(self):

        backup_worker = backup_thread(queue)
#        global store_list  
#       p_dict = {}
#        try:
#            args = self.request.arguments
#            for key in args:
#                value = args[key][0]
#            if value != 'inc' and value != 'full':
#                    raise HTTPAPIError(status_code=-1, error_detail="arguments are wrong",
#                               notification = "direct",
#                               log_message= "arguments are wrong",
#                               response =  "arguments are wrong, retry again.")
#            logging.info("Arguments in Http requests is " + value)
#        except Exception, e:
#            logging.error(e)
#       p_dict.setdefault("flag", value)
        
        backup_worker.flag = "full"
        backup_worker.start()
        sub_p = queue.get()
#       p_dict.setdefault("pid", sub_p)
        
#       print sub_p.pid
        
#        store_list.append(p_dict)
        dict = {}
        #if sub_p == -1: 
#            if value == "inc":
#                raise HTTPAPIError(status_code= 411, error_detail="Increment backup process starts wrong",
#                               notification = "direct",
#                               log_message= "Increment backup process starts wrong",
#                               response =  "Increment backup process starts wrong")
#            else:
#                raise HTTPAPIError(status_code=411, error_detail="Full backup process starts wrong",
#                               notification = "direct",
#                               log_message= "Full backup process starts wrong",
#                               response =  "Full backup process starts wrong")
        #     raise HTTPAPIError(status_code=411, error_detail="Full backup process starts wrong",
#                               notification = "direct",
#                               log_message= "Full backup process starts wrong",
#                               response =  "Full backup process starts wrong")
        #else:
        dict.setdefault("message", "Process is running ,wait")
        self.finish(dict)
         


#eq curl  "http://localhost:8888/backup/check" backup data by full dose.
class BackUpCheck(APIHandler):
#    global store_list
    def get(self):
        date_id = self.get_latest_date_id('/var/log/mcluster-manager/mcluster-backup/')
        logging.info("here")
#       value = p_dict['flag']
#       if value != 'inc' and value != 'full':
#           raise HTTPAPIError(status_code=-1, error_detail="arguments are wrong",
#                             notification = "direct",
#                             log_message= "arguments are wrong",
#                              response =  "arguments are wrong, retry again.")
#       if p_dict['flag'] == "inc":
#          log_filepath = "/var/log/mcluster-manager/mcluster-backup/" + _id + "_script.log" 
#       else :
        log_filepath = "/var/log/mcluster-manager/mcluster-backup/" + date_id +"_script.log"
        dict = {}

                  
        start_lines = os.popen("grep  '== Mysql backup  is starting  ==' " + log_filepath).readlines()
        logging.info(str(start_lines))
        if (len(start_lines) == 1):
            failed_lines = os.popen(" grep '== script is failed ==' " + log_filepath).readlines()
            if (len(failed_lines) != 0):
               logging.error(str(failed_lines))
               raise HTTPAPIError(status_code=411, error_detail=str(failed_lines),
                           notification = "direct",
                           log_message= "Full backup failed",
                           response =  "Full backup failed")
            end_lines = os.popen("grep '== the script is ok ==' "+ log_filepath).readlines()
            if (len(end_lines) == 1):
                dict.setdefault("message", "back up success")
            elif(len(end_lines) == 0):
                dict.setdefault("message", "back up is processing")
            else:
                logging.error(str(endlines))
                raise HTTPAPIError(status_code=411, error_detail="Full backup ended more than one time",
                           notification = "direct",
                           log_message= "Full backup process ended more than one time",
                           response =  "Full backup process ended more than one time")
                
#          if end_flag == True:
#            if len(re_obj) == 0:
#                dict["message"] = "back up failed"
#                    if p_dict['flag'] == "inc":
#                        raise HTTPAPIError(status_code= 411, error_detail="Increment backup process starts wrong",
#                               notification = "direct",
#                               log_message= "Increment backup process starts wrong",
#                               response =  "Increment backup process starts wrong")
#                    else:
        else:
            raise HTTPAPIError(status_code=411, error_detail="Full backup process starts more than one time",
                           notification = "direct",
                           log_message= "Full backup process starts more than one time",
                           response =  "Full backup process starts more than one time")
                    
        self.finish(dict)

    def get_latest_date_id(self, _path):
        list = []
        for f in listdir(_path):
            if(re.search("_script", f) != None):
               date = f.replace("_script.log", "")
               list.append(int(date))
        if (len(list) == 0):
            logging.error("list is empty")
        list.sort()
        return str(list[-1])

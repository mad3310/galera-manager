#-*- coding: utf-8 -*-

import re
import os
import time
import Queue
import socket
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
from common.tornado_basic_auth import require_basic_auth


# Start backing up database data.
#eq curl --user root:root "http://localhost:8888/backup" backup data by full dose.

queue = multiprocessing.Queue()

@require_basic_auth
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
         
#eq curl  "http://localhost:8888/backup/inner/check" backup data by full dose.
class BackUpChecker(APIHandler):

#    global store_list	
    @asynchronous
    def get(self):
        hostname = socket.gethostname()
        obj =  re.search("-n-3", hostname)
        if obj == None:
            self.finish("true")
            return
        date_id = self.get_latest_date_id('/var/log/mcluster-manager/mcluster-backup/')
        if date_id == "empty":
            logging.info("No backup process has run")
            self.finish("expired")
            return
        logging.info("date_id" + str(date_id))
        time_partition_list = []
        time_partition_list = self.resolve_time(date_id)
        log_datetime = datetime.datetime(int(time_partition_list[0]), int(time_partition_list[1]), 
						int(time_partition_list[2]), int(time_partition_list[3]), int(time_partition_list[4]), int(time_partition_list[5]))
       
        now_time = datetime.datetime.now()
        time_expire = datetime.timedelta(hours = 30)
        expire_time = log_datetime + time_expire
        logging.info("expire_time :" +str(expire_time))
        logging.info("now_time :" + str(now_time))
        logging.info("result:" + str(now_time > expire_time))
        status_dict = {}
        if now_time > expire_time:
       #     status_dict.setdefault("status","expired") 
       #     self.zkOper.write_db_backup_info(status_dict)
            self.finish("expired")
        else:
       #     status_dict.setdefault("status","expected")
       #     self.zkOper.write_db_backup_info(status_dict)
            self.finish("true")
        logging.info("backup status:" + str(status_dict))
      
        
    def resolve_time(self, str_time):
        resolve_result = []

        year = str_time[0:4]
        month = str_time[4:6]
        day = str_time[6:8]
        hour = str_time[8:10]
        min = str_time[10:12]
        second = str_time[12:14]
        
        resolve_result.append(year)
        resolve_result.append(month)
        resolve_result.append(day)
        resolve_result.append(hour)
        resolve_result.append(min)
        resolve_result.append(second)
        return resolve_result

    def get_latest_date_id(self, _path):
        list = []
        list_dir = listdir(_path)
        if list_dir == []:
            return "empty"
        for f in list_dir:
            if(re.search("^[0-9]+_script.log$", f) != None):
               date = f.replace("_script.log", "")
               list.append(int(date))
        if (len(list) == 0):
            logging.error("list is empty")
        list.sort()
        return str(list[-1])


#eq curl  "http://localhost:8888/backup/check" backup data by full dose.
class BackUpCheck(APIHandler):

#    global store_list	
    @asynchronous
    def get(self):
        date_id = self.get_latest_date_id('/var/log/mcluster-manager/mcluster-backup/')
        if date_id == "empty":
             raise HTTPAPIError(status_code=411, error_detail="Full backup ended less than one time",
                           notification = "direct",
                           log_message= "Full backup process ended less than one time",
                           response =  "Full backup process ended less than one time")
                

        logging.info("date_id" + str(date_id))
        #value = p_dict['flag']
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
        list_dir = listdir(_path)
        if list_dir == []:
            return "empty"
        for f in list_dir:
            if(re.search("^[0-9]+_script.log$", f) != None):
               date = f.replace("_script.log", "")
               list.append(int(date))
        if (len(list) == 0):
            logging.error("list is empty")
        list.sort()
        return str(list[-1])

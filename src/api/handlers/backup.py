#-*- coding: utf-8 -*-

import re
import os
import time
import Queue
import json
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
from tornado.gen import Callback, Wait
from tornado.options import options
from tornado.httpclient import HTTPRequest
from tornado.web import asynchronous
from tornado import escape
from common.invokeCommand import InvokeCommand
from common.utils.exceptions import HTTPAPIError
from handlers.backup_thread import backup_thread
from common.helper import _retrieve_userName_passwd, is_monitoring
from common.tornado_basic_auth import require_basic_auth

# Start backing up database data.
#eq curl --user root:root "http://localhost:8888/backup" backup data by full dose.

queue = multiprocessing.Queue()

@require_basic_auth
class BackUp(APIHandler):
    
#    global store_list
    @tornado.gen.engine
    @asynchronous
    def get(self):
        dict = {}
        url_post = "/inner/backup"
        online_node_list = self.zkOper.retrieve_started_nodes()
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(socket.gethostname())
        adminUser, adminPasswd = _retrieve_userName_passwd()
        obj =  re.search("-n-3", hostname)
        if obj == None:
            local_ip = socket.gethostbyname(socket.gethostname())
            logging.info("local ip :" + str(local_ip))
            online_node_list.remove(local_ip)
            http_client = tornado.httpclient.AsyncHTTPClient()
            for node_ip in online_node_list:
                requesturi = "http://"+ node_ip +":"+str(options.port)+ url_post
                request = HTTPRequest(url=requesturi, method='GET', auth_username=adminUser, auth_password = adminPasswd)
                logging.info("url is " + requesturi)
                http_client.fetch(request, callback=None)
            dict.setdefault("message", "Process is running ,wait")
        else:
            requesturi = "http://"+ local_ip +":"+str(options.port)+ url_post
            request = HTTPRequest(url=requesturi, method='GET', auth_username=adminUser, auth_password = adminPasswd)
            logging.info("url is " + requesturi)
            http_client.fetch(request, callback=None)
            dict.setdefault("message", "Process is running ,wait")
        self.finish(dict)
 

@require_basic_auth
class BackUper(APIHandler):
    @asynchronous
    def get(self):
       	hostname = socket.gethostname()
        obj = re.search("-n-3", hostname)
        dict = {}
        if obj == None:
            dict.setdefault("message", "not n-3 node")
        else:
            backup_worker = backup_thread(queue)
#        global store_list  
#        p_dict = {}
#       p_dict.setdefault("flag", value)
        
        backup_worker.flag = "full"
        backup_worker.start()
        sub_p = queue.get()
#       p_dict.setdefault("flag", value)
        
            backup_worker.flag = "full"
            backup_worker.start()
            sub_p = queue.get()
#       p_dict.setdefault("pid", sub_p)
        
#       print sub_p.pid
        
#        store_list.append(p_dict)
        
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
        if not is_monitoring():
            self.finish("true")
            return
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

        log_filepath = "/var/log/mcluster-manager/mcluster-backup/" + date_id +"_script.log"
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
            status_dict.setdefault("status","expired") 
       #     self.zkOper.write_db_backup_info(status_dict)
            self.finish("expired")
        else:
            status_dict.setdefault("status","expected")
            flag = "true"
            start_lines = os.popen("grep  '== Mysql backup  is starting  ==' " + log_filepath).readlines()
            #logging.info(str(start_lines))
            if (len(start_lines) == 1):
                failed_lines = os.popen(" grep '== script is failed ==' " + log_filepath).readlines()
                if (len(failed_lines) != 0):
                   logging.error(str(failed_lines))
                   flag = "false"
            end_lines = os.popen("grep '== the script is ok ==' "+ log_filepath).readlines()
            if (len(end_lines) == 1):
                pass
            elif(len(end_lines) == 0):
                pass
       #if hte " == the script is ok == " shows up more than once, we consider backup run out of order"
            else:
                flag = "false"
       #     self.zkOper.write_db_backup_info(status_dict)
            self.finish(flag)
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
        list_dir = []
        try:
            list_dir = listdir(_path)
        except OSError, e:
            logging.info(e)
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
    @tornado.gen.engine
    @asynchronous
    def get(self):
        dict = {}
        url_post = "/backup/checker"
        online_node_list = self.zkOper.retrieve_started_nodes()
        hostname = socket.gethostname()
       
        local_ip = socket.gethostbyname(socket.gethostname())
        logging.info("local ip :" + str(local_ip))
        http_client = tornado.httpclient.AsyncHTTPClient()
        key_sets = set()
        for node_ip in online_node_list:
            requesturi = "http://"+ node_ip +":"+str(options.port)+ url_post
            callback_key = str(node_ip)
            key_sets.add(callback_key)
            request = HTTPRequest(url=requesturi, method='GET' )
            logging.info("url is " + requesturi)
            http_client.fetch(request, callback=(yield Callback(callback_key)))
        
        valid_message = ""
        http_code = 0
        valid_http_code = 0 
        callback = None
        dict = {}
        response_message = []
        for i in range(len(online_node_list)):
            callback_key = key_sets.pop()
            response = yield Wait(callback_key)
            if response.error:
                continue
            else:
                logging.info("response: %s" % str(response.body.strip()))
                response_message.append(json.loads(response.body.strip()))
                
        last_message = ""
        for message in response_message:
            logging.info("response: %s" % str(response))
            if message["meta"]["code"] == 405:
                continue
            if message["meta"]["code"] == 200:
                last_message = escape.json_encode(message)
                break
            else:
                last_message = escape.json_encode(message)

        if not last_message:
            last_message = '{"notification": {"message": "direct"}, "meta": {"code": 404, "errorType": "endpoint_error", "errorDetail": "http socket failed"}, "response": "http socket failed"}'
        self._write_buffer = [last_message]
        self.finish("")

#eq curl "http://localhost:8888/backup/checker"
class BackUp_Checker(APIHandler):

#    global store_list	
    @asynchronous
    def get(self):
        hostname = socket.gethostname()
        obj =  re.search("-n-3", hostname)
        if obj == None:
            raise HTTPAPIError(status_code= 405, error_detail="not n3 node",
                               notification = "direct",
                               log_message= "not n3 node",
                               response =  "not n3 node")
            
        else:
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
            time_partition_list = []
            time_partition_list = self.resolve_time(date_id)

            log_datetime = datetime.datetime(int(time_partition_list[0]), int(time_partition_list[1]), 
					int(time_partition_list[2]), int(time_partition_list[3]), int(time_partition_list[4]), int(time_partition_list[5]))
       
            now_time = datetime.datetime.now()
            time_expire = datetime.timedelta(hours = 30)
            expire_time = log_datetime + time_expire
            logging.info("expire_time :" +str(expire_time))
            if now_time > expire_time:
                 dict.setdefault("message","expired") 
       #     self.zkOper.write_db_backup_info(status_dict)
            else:
                start_lines = os.popen("grep  '== Mysql backup  is starting  ==' " + log_filepath).readlines()
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
                        dict.setdefault("message", "backup success")
                    elif(len(end_lines) == 0):
                        dict.setdefault("message", "back up is processing")
                    else:
                        logging.error(str(end_lines))
                        raise HTTPAPIError(status_code=411, error_detail="Full backup ended more than one time",
                                      notification = "direct",
                                      log_message= "Full backup process ended more than one time",
                                      response =  "Full backup process ended more than one time")
                
                else:
                    raise HTTPAPIError(status_code=411, error_detail="Full backup process starts more than one time",
                                  notification = "direct",
                                  log_message= "Full backup process starts more than one time",
                                  response =  "Full backup process starts more than one time")
                     
            self.finish(dict)

    
    def get_latest_date_id(self, _path):
        list = []
        list_dir = []
        try:
            list_dir = listdir(_path)
        except OSError, e:
            logging.info(e)
            return "empty"
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



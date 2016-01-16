#-*- coding: utf-8 -*-
import re
import os
import json
import socket
import logging
import datetime
import tornado.httpclient

from os import listdir
from base import APIHandler
from tornado.gen import Callback, Wait
from tornado.options import options
from tornado.httpclient import HTTPRequest
from tornado.web import asynchronous
from tornado import escape
from common.utils.exceptions import HTTPAPIError
from common.backup_thread import backup_thread
from common.helper import is_monitoring, get_localhost_ip 
from common.tornado_basic_auth import require_basic_auth
from backup_utils.dispath_backup_worker import DispatchFullBackupWorker, DispatchIncrBackupWorker
from backup_utils.backup_worker import BackupWorkers

# Start backing up database data.
#eq curl --user root:root "http://localhost:8888/backup" backup data by full dose.

def resolve_time(str_time):
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


def get_latest_date_id(_path):
    result_list = []
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
            result_list.append(int(date))
            
    if (len(result_list) == 0):
        logging.error("list is empty")
        
    result_list.sort()
    return str(result_list[-1])

@require_basic_auth
class Full_Backup(APIHandler):
    
    def post(self):
        worker = DispatchFullBackupWorker()
        worker.start()
        
        result = {}
        result.setdefault("message", "Full backup process is running, please waiting")
        self.finish(result)
        
@require_basic_auth
class Incr_Backup(APIHandler):
    
    def post(self):
        incr_basedir = self.get_argument("incr_basedir", None)
        worker = DispatchIncrBackupWorker(incr_basedir)
        worker.start()
        
        result = {}
        result.setdefault("message", "Incr backup process is running, please waiting")
        self.finish(result)
        
        
@require_basic_auth
class Inner_Backup_Action(APIHandler):
    
    def post(self):
        backup_type = self.get_argument("backup_type", None)
        incr_basedir = self.get_arguments("incr_basedir", None) 

        if not backup_type:
            raise HTTPAPIError(status_code=417, error_detail="backup_type params is not transmit",\
                                notification = "direct", \
                                log_message= "backup params is not transmit",\
                                response =  "please check 'backup_type' params.")
            
        backup_worker = BackupWorkers(backup_type, incr_basedir)
        backup_worker.start()
        result = {}
        result.setdefault("message", "Inner backup process is running, please waiting")
        self.finish(result)
 

@require_basic_auth
class BackUper(APIHandler):
    
    @asynchronous
    def get(self):
        hostname = socket.gethostname()
        obj = re.search("-n-3", hostname)
        
        result = {}
        if obj == None:
            result.setdefault("message", "not n-3 node")
        else:
            backup_worker = backup_thread()
            backup_worker.start()
            result.setdefault("message", "Process is running ,wait")
            
        self.finish(result)
         
#eq curl  "http://localhost:8888/backup/inner/check" backup data by full dose.
class BackUpChecker(APIHandler):

    @asynchronous
    def get(self):
        
        zkOper = self.retrieve_zkOper()
        '''
        @todo: is_monitoring no have host_ip?
        '''
        if not is_monitoring(zkOper=zkOper):
            self.finish("true")
            return
        
        hostname = socket.gethostname()
        obj =  re.search("-n-3", hostname)
        if obj == None:
            self.finish("true")
            return
        
        date_id = get_latest_date_id('/var/log/mcluster-manager/mcluster-backup/')
        if date_id == "empty":
            logging.info("No backup process has run")
            self.finish("expired")
            return

        log_filepath = "/var/log/mcluster-manager/mcluster-backup/" + date_id +"_script.log"
        logging.info("date_id" + str(date_id))
        time_partition_list = resolve_time(date_id)
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
            else:
                flag = "false"
                
            self.finish(flag)
        logging.info("backup status:" + str(status_dict))
    
    
#eq curl  "http://localhost:8888/backup/check" backup data by full dose.
class BackUpCheck(APIHandler):
    
    @tornado.gen.engine
    @asynchronous
    def get(self):
        url_post = "/backup/checker"
        
        zkOper = self.retrieve_zkOper()
        online_node_list = zkOper.retrieve_started_nodes()
        
        local_ip = get_localhost_ip()
        logging.info("local ip :" + str(local_ip))
        
        
        http_client = tornado.httpclient.AsyncHTTPClient()
        key_sets = set()
        
        try:
            for node_ip in online_node_list:
                requesturi = "http://"+ node_ip +":"+str(options.port)+ url_post
                callback_key = str(node_ip)
                key_sets.add(callback_key)
                request = HTTPRequest(url=requesturi, method='GET')
                logging.info("url is " + requesturi)
                http_client.fetch(request, callback=(yield Callback(callback_key)))
            
            response_message = []
            for i in range(len(online_node_list)):
                callback_key = key_sets.pop()
                response = yield Wait(callback_key)
                if response.error:
                    continue
                else:
                    logging.info("response: %s" % str(response.body.strip()))
                    response_message.append(json.loads(response.body.strip()))
        finally:
            http_client.close()
            
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
            date_id = get_latest_date_id('/var/log/mcluster-manager/mcluster-backup/')
            if date_id == "empty":
                raise HTTPAPIError(status_code=411, error_detail="Full backup ended less than one time",
                               notification = "direct",
                               log_message= "Full backup process ended less than one time",
                               response =  "Full backup process ended less than one time")
                

            logging.info("date_id" + str(date_id))
            log_filepath = "/var/log/mcluster-manager/mcluster-backup/" + date_id +"_script.log"
            
            time_partition_list = []
            time_partition_list = resolve_time(date_id)

            log_datetime = datetime.datetime(int(time_partition_list[0]), int(time_partition_list[1]), 
					int(time_partition_list[2]), int(time_partition_list[3]), int(time_partition_list[4]), int(time_partition_list[5]))
       
            now_time = datetime.datetime.now()
            time_expire = datetime.timedelta(hours = 30)
            expire_time = log_datetime + time_expire
            logging.info("expire_time :" +str(expire_time))
            
            result = {}
            if now_time > expire_time:
                result.setdefault("message","expired")
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
                        result.setdefault("message", "backup success")
                    elif(len(end_lines) == 0):
                        result.setdefault("message", "backup is processing")
                    else:
                        logging.error(str(end_lines))
                        raise HTTPAPIError(status_code=417, error_detail="Full backup ended more than one time",
                                      notification = "direct",
                                      log_message= "Full backup process ended more than one time",
                                      response =  "Full backup process ended more than one time")
                else:
                    raise HTTPAPIError(status_code=417, error_detail="Full backup process starts more than one time",
                                  notification = "direct",
                                  log_message= "Full backup process starts more than one time",
                                  response =  "Full backup process starts more than one time")
                     
            self.finish(result)

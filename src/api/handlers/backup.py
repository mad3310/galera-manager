#-*- coding: utf-8 -*-
import re

import logging
import datetime

from os import listdir
from base import APIHandler
from common.utils.exceptions import HTTPAPIErrorException
from common.tornado_basic_auth import require_basic_auth
from backup_utils.dispath_backup_worker import DispatchBackupWorker
from backup_utils.backup_worker import BackupWorkers
from backup_utils.base_backup_check import get_response_request, get_local_backup_status
from tornado.web import asynchronous
from tornado.gen import engine
from common.utils.asyc_utils import run_on_executor, run_callback


# Start backing up database data.
#eq curl --user root:root "http://localhost:8888/backup" backup data by full dose.

def resolve_time(str_time):
    resolve_result = []

    year = str_time[0:4]
    month = str_time[4:6]
    day = str_time[6:8]
    hour = str_time[8:10]
    minu = str_time[10:12]
    second = str_time[12:14]
    
    resolve_result.append(year)
    resolve_result.append(month)
    resolve_result.append(day)
    resolve_result.append(hour)
    resolve_result.append(minu)
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

#eg. curl --user root:root -d "backup_type=full" "http://127.0.0.1:8888/backup"
#eg. curl --user root:root -d "backup_type=incr" "http://127.0.0.1:8888/backup"
@require_basic_auth
class Backup(APIHandler):
    
    def post(self):
        incr_basedir = self.get_argument("incr_basedir", None)
        backup_type = self.get_argument("backup_type")
        if not backup_type:
            raise HTTPAPIErrorException("backup params is not given, please check 'backup_type' params.", status_code=417)

        worker = DispatchBackupWorker(backup_type, incr_basedir)
        worker.start()
        
        result = {}
        result.setdefault("message", "backup process is running, please waiting")
        self.finish(result)

#eg. curl --user root:root -d "backup_type=full" "http://127.0.0.1:8888/inner/backup"      
@require_basic_auth
class Inner_Backup_Action(APIHandler):
    
    def post(self):
        backup_type = self.get_argument("backup_type")
        incr_basedir = self.get_arguments("incr_basedir") 

        if not backup_type:
            raise HTTPAPIErrorException("backup params is not transmit, please check 'backup_type' params.", status_code=417)
            
        backup_worker = BackupWorkers(backup_type, incr_basedir)
        backup_worker.start()
        result = {}
        result.setdefault("message", "inner backup process is running, please waiting")
        self.finish(result)
 
    
#eq curl  "http://127.0.0.1:8888/backup/check" 
class BackUpCheck(APIHandler):
    
    @asynchronous
    @engine
    def get(self):
        return_result = yield self._check_backup_stat()  
        self.finish(return_result)
        
    @run_on_executor()
    @run_callback
    def _check_backup_stat(self):
        zkOper = self.retrieve_zkOper()
        backup_info = zkOper.retrieve_backup_status_info()
        
        if not backup_info:
            raise HTTPAPIErrorException("this cluster is not backup, please full backup!", status_code=417)

        if 'recently_backup_ip: ' not in backup_info:
            raise HTTPAPIErrorException("last time backup is not successed", status_code=417)
        
        last_backup_ip = backup_info['recently_backup_ip: ']
        last_backup_time = backup_info['time: ']
        last_backup_type = {'backup_type' : backup_info['backup_type: ']}
        
        time = long(datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
        backup_time = long(datetime.datetime.strptime(last_backup_time, "%Y-%m-%d %H:%M:%S").strftime('%Y%m%d%H%M%S'))
        
        url_post = "/backup/checker"
        result = {}
        if (time - backup_time)/10000 > 30:
            result.setdefault("message", "last backup expired")
        
        else:
            response_message = get_response_request(last_backup_ip, url_post, last_backup_type)
            logging.info(response_message)
            
            if response_message["meta"]["code"] == 417:
                result.setdefault("message", "inner backup interface params transmit failed")
            
            elif response_message["meta"]["code"] == 200:
                message = response_message['response']
                if -1 != message['message'].find('success'):
                    result.setdefault("message", '%s backup success' %last_backup_type['backup_type'])
                if -1 != message['message'].find('starting'):
                    result.setdefault("message", 'last time %s backup is processing' %last_backup_type['backup_type'])
                else:
                    result.setdefault("message", 'last time %s backup failed' %last_backup_type['backup_type'])
            else:
                result.setdefault("message", 'last time %s backup failed' %last_backup_type['backup_type'])

        return result

#eq curl -d "backup_type=full" "http://localhost:8888/backup/checker"
class BackUp_Checker(APIHandler):

    def post(self):
        zkOper = self.retrieve_zkOper()
        backup_type = self.get_argument("backup_type")
        if not backup_type:
            raise HTTPAPIErrorException("backup_type params is not given, please check 'backup_type' params.", status_code=417)
        
        result = {"message": "%s backup is starting" %backup_type}
        backup_info = zkOper.retrieve_type_backup_status_info(backup_type)
        
        if backup_type == 'full':
            backup_start_time = backup_info['backup_start_time:']
            backup_time = datetime.datetime.strptime(backup_start_time, "%Y-%m-%d %H:%M:%S").strftime('%Y%m%d%H%M%S')
            backup_status = backup_info['backup_status:']
            
            local_backup_result = get_local_backup_status(backup_type, backup_time)
            
            if local_backup_result:
                if backup_status == 'backup_succecced':
                    result["message"] = "full backup success"
                
                elif backup_status == 'backup_starting':
                    result["message"] = "full backup is starting"
                    
                elif backup_status == 'backup_failed':
                    result["message"] = "full backup is failed"
        
        else:
            backup_start_time = backup_info['incr_backup_start_time:']
            backup_time = datetime.datetime.strptime(backup_start_time, "%Y-%m-%d %H:%M:%S").strftime('%Y%m%d%H%M%S')
            backup_status = backup_info['incr_backup_status:']
            
            local_backup_result = get_local_backup_status(backup_type, backup_time)
            
            if local_backup_result:
                if backup_status == 'backup_succecced':
                    result["message"] = "incr backup success"
                
                elif backup_status == 'backup_starting':
                    result["message"] = "incr backup is starting" 
                
                elif backup_status == 'backup_failed':
                    result["message"] = "incr backup is failed" 
    
        self.finish(result)

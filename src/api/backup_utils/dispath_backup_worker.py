'''
Created on Sep 28, 2015

@author: root
'''
import threading
import logging
import datetime
from common.utils.exceptions import UserVisiableException
from backup_utils.backup_worker_method import BackupWorkerMethod

TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

class DispatchFullBackupWorker(threading.Thread, BackupWorkerMethod):
    '''
    classdocs
    '''
    def __init__(self):
        threading.Thread.__init__(self)
        BackupWorkerMethod.__init__(self)
        self.data = {}


    def run(self):
        url_path = "/inner/backup"
        self.data['backup_type'] = 'full'
        try:
            action_ips = self._get_usable_ips()
            if not action_ips:
                raise UserVisiableException('no available node, usually disk is not enough!')
            
            self._dispatch_request([action_ips[0]], 'POST', url_path, self.data)
        except Exception, e:
            logging.info(e)


class DispatchIncrBackupWorker(threading.Thread, BackupWorkerMethod):
    '''
    classdocs
    '''
    def __init__(self, incr_basedir):
        threading.Thread.__init__(self)
        self.incr_basedir = incr_basedir
        self.data = {}

    def run(self):
        url_path = "/inner/backup"
        if self.incr_basedir:
            self.data['incr_basedir'] = self.incr_basedir
        self.data['backup_type'] = 'incr'
        try:
            key_value = self.zkOper.retrieve_backup_status_info()
            action_ips = key_value['recently_backup_ip: ']
            if not action_ips:
                raise UserVisiableException('no available full-backup node')
            
            self._dispatch_request(action_ips, 'POST', url_path, self.data)
        except Exception, e:
            logging.info(e) 

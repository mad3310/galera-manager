'''
Created on Sep 28, 2015

@author: root
'''
import threading
import logging
import datetime
from common.utils.exceptions import UserVisiableException
from backup.backup_worker_method import BackupWorkerMethod

class DispatchFullBackupWorker(threading.Thread, BackupWorkerMethod):
    '''
    classdocs
    '''
    def __init__(self):
        threading.Thread.__init__(self)
        self.data = {}

    def run(self):
        url_path = "/inner/backup/full"
        self.data['backup_type'] = 'full'
        try:
            action_ips = self._get_usable_ips()
            if not action_ips:
                raise UserVisiableException('no available node, usually disk is not enough!')
            
            backup_record = {"full_backup_ip: " : action_ips[0], 'time' : datetime.datetime.now()}
            self.zkOper.write_backup_backup_info(backup_record)
            self._dispatch_request(action_ips[0], 'POST', url_path, self.data)
        except Exception, e:
            logging.info(e)


class DispatchIncrBackupWorker(threading.Thread, BackupWorkerMethod):
    '''
    classdocs
    '''
    def __init__(self, incr_basedir):
        threading.Thread.__init__(self)
        self.data['incr_basedir'] = incr_basedir
        self.data['backup_type'] = 'incr'

    def run(self):
        url_path = "/inner/backup/incr"
        try:
            action_ips = self._get_usable_ips(url_path)
            if not action_ips:
                raise UserVisiableException('no available node, usually disk is not enough!')
            
            backup_record = {"incr_backup_ip: " : action_ips[0], 'time' : datetime.datetime.now()}
            self.zkOper.write_backup_backup_info(backup_record)
            self._dispatch_request(action_ips[0], 'POST', url_path, self.data)
        except Exception, e:
            logging.info(e) 

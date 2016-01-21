'''
Created on Sep 28, 2015

@author: xu&zhou
'''
import os
import datetime
from backup_utils.abstract_backup_opers import AbstractBackupOpers
from common.appdefine.backupDefine import *
from tornado.options import options
from common.helper import get_localhost_ip
from backup_utils.status_enum import Status

TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

class FullBackupOpers(AbstractBackupOpers):
    '''
    classdocs
    '''
    
    def __init__(self):
        self.time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        self.path = '/var/log/mcluster-manager/mcluster-backup/'
        self.file_name = self.time + '_script.log'
        self.status = {}


    def remove_expired_backup_file(self):
        self._delete_file(REMOTE_BACKUPDIR, days_count=4)
    
    def create_backup_directory(self):
        [self._run_comm_call('mkdir -p %s' %path) for path in (BACKUPDIR, REMOTE_BACKUPDIR, LOG_FILE_PATH)]

    
    def backup_action(self, ZkOpers):
        
        record = '%s  == Mysql backup  is starting  == ' %datetime.datetime.now().strftime(TIME_FORMAT)
        
        self.status['backup_status:'] = Status.backup_starting
        self.status['backup_status_time:'] = datetime.datetime.now().strftime(TIME_FORMAT)
        
        self._write_info_to_local(self.path, self.file_name, record)
        ZkOpers.write_backup_fullbackup_info(self.status)
        
        bak_cmd = "innobackupex --user=%s --password=%s \
                    --defaults-file=%s --no-timestamp \
                    %s/full_backup-%s >> %s/%s_backup.log 2>&1" \
                    % (BACKUP_USER, BACKUP_PASSWD, \
                       options.mysql_cnf_file_name, \
                       BACKUPDIR, self.time, LOG_FILE_PATH, \
                       self.time)
          
        run_bak_result = os.system(bak_cmd)
        
        '''
            @todo: use backup's logger
        '''

        if 0 == run_bak_result:
            record = '%s  == Backup All Data end == ' %datetime.datetime.now().strftime(TIME_FORMAT)

            self.status['backup_status:'] = Status.backup_succecced
            self.status['backup_status_time:'] = datetime.datetime.now().strftime(TIME_FORMAT)
             
            self._write_info_to_local(self.path, self.file_name, record)
            ZkOpers.write_backup_fullbackup_info(self.status)

        else:
            record = '%s  == Backup All Data is ERROR == ' %datetime.datetime.now().strftime(TIME_FORMAT)
            self.status['backup_status:'] = Status.backup_failed
            self.status['backup_status_time:'] = datetime.datetime.now().strftime(TIME_FORMAT)
            
            self._write_info_to_local(self.path, self.file_name, record)
            ZkOpers.write_backup_fullbackup_info(self.status)
            return 
    
    def trans_backup_file(self, ZkOpers):
        rsync_cmd = """rsync -azvr --include "full_backup-%s/" --exclude "/*" --bwlimit=18840 %s/* %s/""" % (self.time, BACKUPDIR, REMOTE_BACKUPDIR)             
        run_rsync_result = os.system(rsync_cmd)
        
        if 0 == run_rsync_result:

            self._fb_update_index('/full_backup-' + self.time)
            
            record = '%s  == Cp backup_file ok == ' %datetime.datetime.now().strftime(TIME_FORMAT)
            self.status['cp_status:'] = Status.backup_transmit_succeed
            self.status['full_backup_ip:'] = str(get_localhost_ip())
            self.status['cp_status_time:'] = datetime.datetime.now().strftime(TIME_FORMAT)

            self._write_info_to_local(self.path, self.file_name, record)
            ZkOpers.write_backup_fullbackup_info(self.status)

        else:
            record = '%s  == backup_file is not cp /data == ' %datetime.datetime.now().strftime(TIME_FORMAT)
            self.status['cp_status:'] = Status.backup_transmit_faild
            self.status['backup_ip:'] = None
            self.status['cp_status_time:'] = datetime.datetime.now().strftime(TIME_FORMAT)
            
            self._write_info_to_local(self.path, self.file_name, record)
            ZkOpers.write_backup_fullbackup_info(self.status)

        
'''
Created on Sep 28, 2015

@author: xu&zhou
'''
import os
import datetime
from backup_utils.abstract_backup_opers import AbstractBackupOpers
from common.appdefine.backupDefine import *
from tornado.options import options
from backup_utils.status_enum import Status


class FullBackupOpers(AbstractBackupOpers):
    '''
    classdocs
    '''
    
    def __init__(self):
        self.file_name = '**_full_script.log'

    def remove_expired_backup_file(self):
        [self._delete_file(directory) for directory in (REMOTE_BACKUPDIR, BACKUPDIR)]
    
    def create_backup_directory(self):
        [self._run_comm_call('mkdir -p %s' %path) for path in (BACKUPDIR, REMOTE_BACKUPDIR, LOG_FILE_PATH)]

    
    def backup_action(self, ZkOpers, time):

        self.backup_record.setdefalut(Status.backup_starting, '== Mysql backup  is starting  == %s' %datetime.datetime.now())
        self.status['backup_status: '] = Status.backup_starting  + ' ' + datetime.datetime.now()
        
        self._write_info_to_local(self.file_name, self.backup_record)
        ZkOpers.write_backup_fullbackup_info(self.status)
        
        self.file_name = time + '_full_script.log'
        
        bak_cmd = "innobackupex --user=%s --password=%s \
                    --defaults-file=%s --no-timestamp \
                    %s/full_backup-%s >> %s/%s_backup.log 2>&1" \
                    % (BACKUP_USER, BACKUP_PASSWD, \
                       options.mysql_cnf_file_name, \
                       BACKUPDIR, time, LOG_FILE_PATH, \
                       time)
          
        run_bak_result = os.system(bak_cmd)
        
        '''
            @todo: use backup's logger
        '''

        if '0' == run_bak_result:
            self.backup_record.setdefalut(Status.backup_succecced, '== Backup All Data end == %s' %datetime.datetime.now())

            self.status['backup_status: '] = Status.backup_succecced + ' ' + datetime.datetime.now()
            
            self._write_info_to_local(self.file_name, self.backup_record)
            ZkOpers.write_backup_fullbackup_info(self.status)

        else:
            self.backup_record.setdefalut(Status.backup_failed, '== Backup All Data is ERROR %s' %datetime.datetime.now())
            self.status['backup_status: '] = Status.backup_failed + ' ' + datetime.datetime.now()
            
            self._write_info_to_local(self.file_name, self.backup_record)
            ZkOpers.write_backup_fullbackup_info(self.status)
            return 
    
    def trans_backup_file(self, ZkOpers, time):
        rsync_cmd = """rsync -azvr --include "full_backup-%s/ --exclude "/*" --bwlimit=18840 %s/* %s/""" % (time, BACKUPDIR, REMOTE_BACKUPDIR)             
        run_rsync_result = os.system(rsync_cmd)
        
        if '0' == run_rsync_result:
            backup_rs_path_cmd = 'rm -rf ' + BACKUPDIR + '/full_backup-' + time
            
            self._fb_update_index(REMOTE_BACKUPDIR + '/full_backup-' + time)
            self._run_comm_call(backup_rs_path_cmd)
            
            self.backup_record.setdefalut(Status.backup_transmit_succeed, '== Cp backup_file ok %s' %datetime.datetime.now())
            self.status['cp_status: '] = Status.backup_transmit_succeed + ' ' + datetime.datetime.now()
    
            self._write_info_to_local(self.file_name, self.backup_record)
            ZkOpers.write_backup_fullbackup_info(self.status)

        else:
            self.backup_record.setdefalut(Status.backup_transmit_faild, '== backup_file is not cp /data %s' %datetime.datetime.now())
            self.status['cp_status: '] = Status.backup_transmit_faild + ' ' + datetime.datetime.now()

            self._write_info_to_local(self.file_name, self.backup_record)
            ZkOpers.write_backup_fullbackup_info(self.status)

        
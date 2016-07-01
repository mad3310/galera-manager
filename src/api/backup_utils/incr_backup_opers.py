'''
Created on jan.15, 2015

@author: yanwei xu
'''

import os
import datetime
from backup_utils.abstract_backup_opers import AbstractBackupOpers
from common.appdefine.backupDefine import *
from tornado.options import options
from common.helper import get_localhost_ip
from backup_utils.status_enum import Status

TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

class IncrementBackupOpers(AbstractBackupOpers):
    '''
    classdocs
    '''
    #LATEST_FULL='find %s/full -mindepth 1 -maxdepth 1 -type d -printf "%P\n" | sort -nr | head -1'%REMOTE_BACKUPDIR
    def __init__(self, incr_basedir):
        self.base_dir = incr_basedir
        self.time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        self.path = '/var/log/mcluster-manager/mcluster-backup/incr/'
        self.file_name = self.time + '_incr_script.log'
        self.status = {}
        
    def remove_expired_backup_file(self):
        self._delete_file(REMOTE_INCRBACKUPDIR, days_count=8)
    
    def create_backup_directory(self):
        [self._run_comm_call('mkdir -p %s' %path) for path in (INCRBACKUPDIR, REMOTE_INCRBACKUPDIR, TMPFILEDIR)]
    
    def backup_action(self, zkOpers):
        now_time = datetime.datetime.now()
        self.time = now_time.strftime('%Y%m%d%H%M%S')
        self.file_name = self.time + '_incr_script.log'
        
        record = '%s  == incremental backup is starting  == ' %now_time.strftime(TIME_FORMAT)
        self.status['incr_backup_status:'] = Status.backup_starting
        self.status['incr_backup_start_time:'] = now_time.strftime(TIME_FORMAT)
        
        self._write_info_to_local(self.path, self.file_name, record)
        zkOpers.write_backup_innerbackup_info(self.status)
        
        if self.base_dir:
            base_dir = REMOTE_BACKUPDIR + self.basedir
        else:
            base_dir = REMOTE_BACKUPDIR

        bak_cmd = "innobackupex --user=%s --password=%s \
                    --defaults-file=%s --no-timestamp \
                    --incremental %s/incre_backup-%s \
                    --incremental-basedir=%s/full > %s.log \
                    2>&1" % (BACKUP_USER, BACKUP_PASSWD, \
                            options.mysql_cnf_file_name, \
                            INCRBACKUPDIR, self.time, 
                            base_dir, TMPFILE+self.time)

        run_bak_relust = os.system(bak_cmd)
        
        now_time = datetime.datetime.now()
        
        if 0 == run_bak_relust:
            record = '%s  == incr backup all data end  == ' %now_time.strftime(TIME_FORMAT)
            self.status['incr_backup_status:'] = Status.backup_succecced
            self.status['incr_finish_time:'] = now_time.strftime(TIME_FORMAT)
            
            self._write_info_to_local(self.path, self.file_name, record)
            zkOpers.write_backup_innerbackup_info(self.status)
            
        else:
            record = '%s  == incr backup all data is error  == ' %now_time.strftime(TIME_FORMAT)
            self.status['incr_backup_status:'] = Status.backup_failed
            self.status['incr_finish_time:'] = now_time.strftime(TIME_FORMAT)
            self._write_info_to_local(self.path, self.file_name, record)
            zkOpers.write_backup_innerbackup_info(self.status)
            return 
    
    def trans_backup_file(self, zkOpers):
        now_time = datetime.datetime.now()
        record = '%s  == cp incr_backup_file is starting  == ' %now_time.strftime(TIME_FORMAT)
        
        self.status['cp_incr_file_status:'] = Status.backup_transmit_starting
        self.status['cp_incr_file_start_time:'] = now_time.strftime(TIME_FORMAT)
        
        self._write_info_to_local(self.path, self.file_name, record)
        zkOpers.write_backup_innerbackup_info(self.status)
        
        rsync_cmd = """rsync -azvr --include "incre_backup-%s/" --exclude "/*" --bwlimit=18840 %s/* %s/""" % (self.time, INCRBACKUPDIR, REMOTE_INCRBACKUPDIR)
        run_rsync_relust = os.system(rsync_cmd)
        
        now_time = datetime.datetime.now()
        if 0 == run_rsync_relust:
            in_backup_rs_path_cmd = 'rm -rf ' + INCRBACKUPDIR + '/incre_backup-' + self.time
            self._run_comm_call(in_backup_rs_path_cmd)
            
            record = '%s  == cp incr_backup_file ok  == ' %now_time.strftime(TIME_FORMAT)
            
            self.status['cp_incr_file_status:'] = Status.backup_transmit_succeed
            self.status['incr_backup_ip:'] = str(get_localhost_ip())
            self.status['cp_incr_file_finish_time:'] = now_time.strftime(TIME_FORMAT)
            
            self._write_info_to_local(self.path, self.file_name, record)
            zkOpers.write_backup_innerbackup_info(self.status)
            
        else:
            record = '%s  == incr_backup_file is not cp /data == ' %now_time.strftime(TIME_FORMAT)
            self.status['cp_incr_status:'] = Status.backup_transmit_faild
            self.status['cp_incr_finish_time:'] = now_time.strftime(TIME_FORMAT)
            
            self._write_info_to_local(self.path, self.file_name, record)
            zkOpers.write_backup_innerbackup_info(self.status)
            return
            
        record = '%s  == the incr backup is completed == ' %now_time.strftime(TIME_FORMAT)
        self._write_info_to_local(self.path, self.file_name, record)

        self._delete_file(LOG_FILE_PATH + '/incr', days_count=8)

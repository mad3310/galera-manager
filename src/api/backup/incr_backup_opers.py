'''
Created on Sep 28, 2015

@author: root
'''
import logging
import os
import datetime
from backup.abstract_backup_opers import AbstractBackupOpers
from common.appdefine.backupDefine import *
from tornado.options import options
from backup.status_enum import Status


class IncrementBackupOpers(AbstractBackupOpers):
    '''
    classdocs
    '''
    LATEST_FULL='find %s/full -mindepth 1 -maxdepth 1 -type d -printf "%P\n" | sort -nr | head -1'%REMOTE_BACKUPDIR
    def __init__(self, incr_basedir):
        self.basedir = incr_basedir
        self.time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        self.file_name = self.time + '_incr_script.log'
        
    def remove_expired_backup_file(self):
        [self._delete_file(_path) for _path in (INCRBACKUPDIR, REMOTE_INCRBACKUPDIR)]
    
    def create_backup_directory(self):
        [self._run_comm_call('mkdir -p %s' %path) for path in (INCRBACKUPDIR, REMOTE_INCRBACKUPDIR, TMPFILE)]
    
    def backup_action(self, zkOpers, time):

        self.file_name = time + '_incr_script.log'

        if self.base_dir != None:
            base_dir = REMOTE_BACKUPDIR + self.basedir
        else:
            base_dir = REMOTE_BACKUPDIR

        bak_cmd = "innobackupex --user=%s --password=%s \
                    --defaults-file=%s --no-timestamp \
                    --incremental %s/incre_backup-%s \
                    --incremental-basedir=%s/full > %s \
                    2>&1" % (BACKUP_USER, BACKUP_PASSWD, \
                            options.mysql_cnf_file_name, \
                            INCRBACKUPDIR, time, 
                            base_dir, TMPFILE)

        run_bak_relust = os.system(bak_cmd)
        
        if run_bak_relust == '0':
            self.backup_record.setdefalut(Status.backup_succecced, '== Incr backup all data end == %s' %datetime.datetime.now())
            self.status['incr_backup_status: '] = Status.backup_succecced + ' ' + datetime.datetime.now()
            
            self._write_info_to_local(self.file_name, self.backup_record)
            zkOpers.write_backup_innerbackup_info(self.status)
            
        else:
            self.backup_record.setdefalut(Status.backup_failed, '== Incr backup all data is error == %s' %datetime.datetime.now())
            self.status['incr_backup_status: '] = Status.backup_failed + ' ' + datetime.datetime.now()
            
            self._write_info_to_local(self.file_name, self.backup_record)
            zkOpers.write_backup_innerbackup_info(self.status)

            return 
    
    def trans_backup_file(self, ZkOpers, time):
        rsync_cmd = """rsync -azvr --include "increment_backup-%s/ --exclude "/*" --bwlimit=18840 %s/* %s/""" % (time, INCRBACKUPDIR, REMOTE_INCRBACKUPDIR)
        run_rsync_relust = self.__run_comm_call(rsync_cmd)
        
        if '0' == run_rsync_relust:
            in_backup_rs_path_cmd = 'rm -rf ' + INCRBACKUPDIR + '/incre_backup-' + time

            self.backup_record.setdefalut(Status.backup_transmit_succeed, '== Cp incr_backup_file ok == %s' %datetime.datetime.now())
            self.status['cp_status: '] = Status.backup_transmit_succeed + ' ' + datetime.datetime.now()

            self._write_info_to_local(self.file_name, self.backup_record)
            ZkOpers.write_backup_innerbackup_info(self.status)
            
            self.__run_comm_call(in_backup_rs_path_cmd)
        else:
            self.backup_record.setdefalut(Status.backup_transmit_faild, '== Incr_backup_file is not cp /data == %s' %datetime.datetime.now())
            self.status['cp_status: '] = Status.backup_transmit_faild + ' ' + datetime.datetime.now()

            self._write_info_to_local(self.file_name, self.backup_record)
            ZkOpers.write_backup_innerbackup_info(self.status)
            
        
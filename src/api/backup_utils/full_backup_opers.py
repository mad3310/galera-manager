# coding=utf-8
'''
Created on jan 20, 2015

@author: yanwei xu&zhou bingzheng
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
        # 执行备份流程的日志存放目录
        self.path = '/var/log/mcluster-manager/mcluster-backup/'
        self.file_name = self.time + '_script.log'
        self.status = {}


    def remove_expired_backup_file(self):
        self._delete_file(REMOTE_BACKUPDIR, days_count=4)

    def create_backup_directory(self):
        [self._run_comm_call('mkdir -p %s' %path) for path in (BACKUPDIR, REMOTE_BACKUPDIR, LOG_FILE_PATH)]


    def backup_action(self, ZkOpers):
        now_time = datetime.datetime.now()
        self.time = now_time.strftime('%Y%m%d%H%M%S')
        self.file_name = self.time + '_script.log'

        record = '%s  == Mysql backup  is starting  == ' %now_time.strftime(TIME_FORMAT)

        self.status['backup_status:'] = Status.backup_starting
        self.status['backup_start_time:'] = now_time.strftime(TIME_FORMAT)

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
            @todo: use backup's logger. 不明确该TODO，忽略（denglj，xyw，2016/6/30）
        '''
        now_time = datetime.datetime.now()
        if 0 == run_bak_result:
            record = '%s  == Backup All Data end == ' %now_time.strftime(TIME_FORMAT)

            self.status['backup_status:'] = Status.backup_succecced
            self.status['backup_finish_time:'] = now_time.strftime(TIME_FORMAT)

            self._write_info_to_local(self.path, self.file_name, record)
            ZkOpers.write_backup_fullbackup_info(self.status)

        else:
            record = '%s  == Backup All Data is ERROR == ' %now_time.strftime(TIME_FORMAT)
            self.status['backup_status:'] = Status.backup_failed
            self.status['backup_finish_time:'] = now_time.strftime(TIME_FORMAT)

            self._write_info_to_local(self.path, self.file_name, record)
            ZkOpers.write_backup_fullbackup_info(self.status)

    def trans_backup_file(self, ZkOpers):
        now_time = datetime.datetime.now()
        record = '%s  == cp backup_file is starting  == ' %now_time.strftime(TIME_FORMAT)

        self.status['cp_file_status:'] = Status.backup_transmit_starting
        self.status['cp_file_start_time:'] = now_time.strftime(TIME_FORMAT)

        self._write_info_to_local(self.path, self.file_name, record)
        ZkOpers.write_backup_fullbackup_info(self.status)

        rsync_cmd = """rsync -azvr --include "full_backup-%s/" --exclude "/*" --bwlimit=18840 %s/* %s/""" % (self.time, BACKUPDIR, REMOTE_BACKUPDIR)
        run_rsync_result = os.system(rsync_cmd)

        now_time = datetime.datetime.now()
        if 0 == run_rsync_result:
            self._fb_update_index('/full_backup-' + self.time)

            record = '%s  == Cp backup_file ok == ' %now_time.strftime(TIME_FORMAT)
            self.status['cp_status:'] = Status.backup_transmit_succeed
            self.status['full_backup_ip:'] = str(get_localhost_ip())
            self.status['cp_finish_time:'] = now_time.strftime(TIME_FORMAT)

            self._write_info_to_local(self.path, self.file_name, record)
            ZkOpers.write_backup_fullbackup_info(self.status)

        else:
            record = '%s  == backup_file is not cp /data == ' %now_time.strftime(TIME_FORMAT)
            self.status['cp_status:'] = Status.backup_transmit_faild
            self.status['backup_ip:'] = None
            self.status['cp_finish_time:'] = now_time.strftime(TIME_FORMAT)

            self._write_info_to_local(self.path, self.file_name, record)
            ZkOpers.write_backup_fullbackup_info(self.status)
            return

        record = '%s  == the full backup is completed == ' %now_time.strftime(TIME_FORMAT)
        self._write_info_to_local(self.path, self.file_name, record)

        self._delete_file(LOG_FILE_PATH)


# coding=utf8
'''
Created on Sep 28, 2015

@author: root
'''
import threading
import logging
from common.utils.exceptions import UserVisiableException
from backup_utils.backup_worker_method import BackupWorkerMethod


class DispatchBackupWorker(threading.Thread, BackupWorkerMethod):
    '''分发备份任务。
    若为全量备份则先从集群中根据各节点负载获取可用IP，
    增量备份则直接在上一次全量备份成功的节点上继续进行。
    '''
    def __init__(self, backup_type, incr_basedir=None):
        threading.Thread.__init__(self)
        BackupWorkerMethod.__init__(self)
        self.incr_basedir = incr_basedir
        self.backup_type = backup_type
        self.data = {}

    def run(self):
        url_path = "/inner/backup"
        if self.incr_basedir:
            self.data['incr_basedir'] = self.incr_basedir
        self.data['backup_type'] = self.backup_type
        try:
            if self.backup_type == 'full':
                action_ips = self._get_usable_ips()
                if not action_ips:
                    raise UserVisiableException('no available node, usually disk is not enough!')
                self._dispatch_request([action_ips[0]], 'POST', url_path, self.data)
            elif self.backup_type == 'incr':
                key_value = self.zkOper.retrieve_type_backup_status_info('full')
                action_ips = key_value['full_backup_ip:']
                if not action_ips:
                    raise UserVisiableException('no available full-backup node')
                self._dispatch_request([action_ips], 'POST', url_path, self.data)

        except Exception, e:
            logging.info(e)

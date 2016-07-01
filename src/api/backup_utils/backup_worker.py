#!/usr/bin/env python
import threading
import logging
import datetime

from backup_utils.full_backup_opers import FullBackupOpers
from backup_utils.incr_backup_opers import IncrementBackupOpers
from common.zkOpers import Requests_ZkOpers
from common.dba_opers import DBAOpers
from common.helper import retrieve_monitor_password, retrieve_directory_available, retrieve_directory_capacity
from common.helper import get_localhost_ip
from common.invokeCommand import InvokeCommand
from common.utils.exceptions import UserVisiableException


TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

CHECK_DMP_DATA_CMD = 'df -hmP|grep "/data"|wc -l'

class BackupWorkers(threading.Thread):

    zkOpers = Requests_ZkOpers()

    dba_opers = DBAOpers()

    def __init__(self, backup_mode='full', incr_basedir=None):

        self._backup_mode = backup_mode
        self.backup_record = {}
        threading.Thread.__init__(self)

        if self._backup_mode == "full":
            self.backupOpers = FullBackupOpers()
        else:
            self.backupOpers = IncrementBackupOpers(incr_basedir)


    def run(self):
        isLock, lock = self.zkOpers.lock_backup_action()
        if not isLock:
            logging.info('zk is not lock')
            return

        try:
            _password = retrieve_monitor_password()
            conn = self.dba_opers.get_mysql_connection(user="monitor", passwd=_password)
            if None == conn:
                raise UserVisiableException("Can\'t connect to mysql server")

            db_status = self.dba_opers.show_status(conn)
            if 'Synced' != db_status[-14][1]:
                self.backup_record['error: '] = 'Mcluster is not start %s' %datetime.datetime.now().strftime(TIME_FORMAT)
                self.backupOpers._write_info_to_local(self.backupOpers.path, self.backupOpers.file_name, self.backup_record)
                self.zkOpers.write_backup_backup_info(self.backup_record)
                return

            if '0' == self.__run_comm(CHECK_DMP_DATA_CMD):
                self.backup_record['error: '] = 'No have /data partition %s' %datetime.datetime.now().strftime(TIME_FORMAT)
                self.backupOpers._write_info_to_local(self.backupOpers.path, self.backupOpers.file_name, self.backup_record)
                self.zkOpers.write_backup_backup_info(self.backup_record)
                return

            self.backupOpers.create_backup_directory()
            self.backupOpers.remove_expired_backup_file()

            self.backupOpers.backup_action(self.zkOpers)
            self.backupOpers.trans_backup_file(self.zkOpers)

            record = {"recently_backup_ip: " : str(get_localhost_ip()), 'time: ' : datetime.datetime.now().strftime(TIME_FORMAT), 'backup_type: ': self._backup_mode}
            self.zkOpers.write_backup_backup_info(record)

        except Exception, e:
            record = {"error: " : 'backup is wrong, please check it!', 'time:' : datetime.datetime.now().strftime(TIME_FORMAT), 'backup_type: ': self._backup_mode}
            self.zkOpers.write_backup_backup_info(record)
            logging.info(e)

        finally:
            conn.close()
            self.zkOpers.unLock_init_node_action(lock)

    def __run_comm(self, cmdstr):
        invokeCommand = InvokeCommand()
        return_result = invokeCommand._runSysCmd(cmdstr)
        return str(return_result[0])



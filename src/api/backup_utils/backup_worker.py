#!/usr/bin/env python
import threading
import logging
import datetime

from backup_utils.full_backup_opers import FullBackupOpers
from backup_utils.incr_backup_opers import IncrementBackupOpers
from common.zkOpers import Requests_ZkOpers
from common.dba_opers import DBAOpers
from common.helper import retrieve_monitor_password, retrieve_directory_available, retrieve_directory_capacity

from common.invokeCommand import InvokeCommand
from common.utils.exceptions import UserVisiableException


TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

CHECK_DMP_DATA_CMD = 'df -hmP|grep "/data"|wc -l'

class BackupWorkers(threading.Thread):
    
    zkOpers = Requests_ZkOpers()
    
    dba_opers = DBAOpers()
    
    def __init__(self, backup_mode='full', incr_basedir=None):
        
        self._backup_mode = backup_mode
        
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
            conn = self.dba_opers.get_mysql_connection(data_node_ip='127.0.0.1', user="monitor", passwd=_password)
            if None == conn:
                raise UserVisiableException("Can\'t connect to mysql server")
            
            db_status = self.dba_opers.show_status(conn)
            logging.info(db_status[-14][1])
            if 'Synced' != db_status[-14][1]:
                self.backup_record['error: '] = 'Mcluster is not start %s' %datetime.datetime.now()
                self.backupOpers._write_info_to_local(self.backupOpers.file_name, self.backup_record)
                return 
            
            
            if '0' == self.__run_comm(CHECK_DMP_DATA_CMD):
                self.backup_record['error: '] = 'No have /data partition %s' %datetime.datetime.now()
                self.backupOpers._write_info_to_local(self.backupOpers.file_name, self.backup_record)
                return 
            
            mcluster_disk_available = retrieve_directory_available("/srv/mcluster")
            data_disk_available = retrieve_directory_available("/data")
            
            mysql_data_directory_capacity = retrieve_directory_capacity("/srv/mcluster/mysql")
            calculation_data_directory_capacity = mysql_data_directory_capacity * 2
            
            if mcluster_disk_available <= calculation_data_directory_capacity or data_disk_available <= calculation_data_directory_capacity:
                self.backup_record['error: '] = 'The disk is full %s' %datetime.datetime.now()
                self.backupOpers._write_info_to_local(self.backupOpers.file_name, self.backup_record)
                #return 

            self.backupOpers.remove_expired_backup_file()
            self.backupOpers.create_backup_directory()
            
            time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
            
            self.backupOpers.backup_action(self.zkOpers, time)
            self.backupOpers.trans_backup_file(self.zkOpers, time)
            
            logging.info("== Backup completed ==")
        except:
            '''
            @todo: put exception to queue
            '''
            logging.info("backup is wrong, please check it!")
        finally:
            conn.close()
            if lock is not None:
                self.zkOpers.unLock_backup_action(lock)
            
    def __run_comm(self, cmdstr):
        invokeCommand = InvokeCommand()
        return_result = invokeCommand._runSysCmd(cmdstr)
        return str(return_result[0])

    
# -*- coding: utf-8 -*-

import os
import subprocess
import datetime

from abc import abstractmethod

from .consts import BACKUP_CONFIG


class AbstractBackupOpers(object):
    @abstractmethod
    def remove_expired_backup_file(self):
        raise NotImplementedError, "Cannot call abstract method"

    @abstractmethod
    def create_backup_directory(self):
        raise NotImplementedError, "Cannot call abstract method"

    @abstractmethod
    def backup_action(self):
        raise NotImplementedError, "Cannot call abstract method"

    @abstractmethod
    def trans_backup_file(self):
        raise NotImplementedError, "Cannot call abstract method"

    def _run_comm_call(self, cmdStr):
        return_code = subprocess.call(cmdStr, shell=True)
        return str(return_code)

    def _fb_update_index(self, file_name):
        remove_remote_file = 'rm -rf ' + BACKUP_CONFIG.FULL_REMOTE_DIR + '/full'
        ln_sym = 'ln -s ' + BACKUP_CONFIG.FULL_REMOTE_DIR + '/' + file_name + ' ' + \
                 BACKUP_CONFIG.FULL_REMOTE_DIR + '/full'
        remove_local_file = 'rm -rf ' + BACKUP_CONFIG.FULL_LOCAL_DIR + file_name

        [self._run_comm_call(cmdStr) for cmdStr in (remove_remote_file, ln_sym, remove_local_file)]

    def _log_create_time(self):
        log_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        return log_time

    def _write_info_to_local(self, path, filename, key_value):
        if os.path.exists(path) is False:
            os.mkdir(path)

        with open(path + filename, 'a') as f_obj:
            if type(key_value) == dict:
                for key, value in key_value.items():
                    f_obj.write(str(key) + ': ' + str(value))
            else:
                f_obj.write(key_value + '\n')

    def _write_info_to_zk(self, zkOpers, key_value):
        time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        key_value['time'] = time
        zkOpers.write_db_backup_info(key_value)

    def _delete_file(self, backup_path, days_count=4):
        xDate = self.get_day_of_day(days_count)
        expDate = xDate.strftime('%Y%m%d%H%M%S')
        romote_files = os.listdir(backup_path)

        for file_name in romote_files:
            if file_name != 'full' and file_name != 'incr':
                if file_name.find('-') == -1:
                    if file_name.split('_')[0] < expDate:
                        os.system('rm -rf %s/%s' % (backup_path, file_name))
                else:
                    if file_name.split('-')[1] < expDate:
                        os.system('rm -rf %s/%s' % (backup_path, file_name))

    def get_day_of_day(self, n):
        if n > 0:
            return datetime.datetime.today() - datetime.timedelta(days=n)

        return datetime.datetime.today() + datetime.timedelta(days=abs(n))

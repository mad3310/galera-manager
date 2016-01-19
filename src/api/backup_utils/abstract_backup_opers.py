'''
Created on Sep 28, 2015

@author: root
'''
import os, glob, time
import logging
import subprocess
import datetime
from abc import abstractmethod
from common.appdefine.backupDefine import *
import shutil



class AbstractBackupOpers(object):
    '''
    classdocs
    '''


    def __init__(self, params):
        '''
        '''
        
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
        remove_remote_file = 'rm -rf ' + REMOTE_BACKUPDIR + '/full'
        ln_sym = 'ln -s ' + REMOTE_BACKUPDIR +'/' + file_name + ' ' + REMOTE_BACKUPDIR + '/full'
        remove_local_file = 'rm -rf ' + BACKUPDIR + file_name
        
        [self._run_comm_call(cmdStr) for cmdStr in (remove_remote_file, ln_sym, remove_local_file)]
        
    def _log_create_time(self):
        log_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        return log_time
    
    def _write_info_to_local(self, path, filename, key_value):
        if os.path.exists(path) is False:
            os.mkdir(path)
        with open(path + filename, 'a') as f_obj:
            f_obj.write(key_value + '\n')
        
    def _write_info_to_zk(self, zkOpers, key_value):
        time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        key_value['time'] = time
        zkOpers.write_db_backup_info(key_value)

    def _delete_file(self, backup_path):

        xDate = self.get_day_of_day(4)
        #expDate = time.strptime(xDate, '%Y-%m-%d')
        expDate = xDate.strftime('%Y%m%d%H%M%S')
        
        for backup_folder in glob.glob(backup_path):
            # retrieves the stats for the current folder
            # the tuple element at index 8 is the last-modified-date
            stats = os.stat(backup_folder)
            # put the two dates into matching format
            lastmodDate = time.strftime("%Y%m%d%H%M%S", time.localtime(stats[8]))
            
            logging.info(lastmodDate)
            # check if image-last-modified-date is outdated
            if expDate > lastmodDate:
                #logging.info('removed %s', backup_folder)
                os.remove(backup_folder) # commented out for testing

    def get_day_of_day(self, n=0):
        '''''
        if n>=0,date is larger than today
        if n<0,date is less than today
        date format = "YYYY-MM-DD"
        '''
        if(n<0):
            n = abs(n)
            return datetime.datetime.today() - datetime.timedelta(days=n)
        else:
            return datetime.datetime.today() + datetime.timedelta(days=n)


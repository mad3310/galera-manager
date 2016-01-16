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
    
    def _fb_update_index(self, re_path):
        backup_path = "%s/full" % (re_path)
        os.remove(backup_path)
        os.link(backup_path, re_path)
        
    def _log_create_time(self):
        log_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        return log_time
    
    def _write_info_to_local(self, filename, key_value):
        with open(filename, 'a') as f_obj:
            f_obj.writelines(key_value)
        
    def _write_info_to_zk(self, zkOpers, key_value):
        time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        key_value['time'] = time
        zkOpers.write_db_backup_info(key_value)

    def _delete_file(self, backup_path):
        # remove all jpeg image files of an expired modification date = mtime
        # you could also use creation date (ctime) or last access date (atime)
        # os.stat(filename) returns (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime)
        # tested with Python24  vegaseat 6/7/2005
        
        # expiration date in the format YYYY-MM-DD
        xDate = self.get_day_of_day(4)
        #expDate = time.strptime(xDate, '%Y-%m-%d')
        expDate = xDate.strftime('%Y%m%d%H%M%S')
        
        for backup_folder in glob.glob(backup_path):
            # retrieves the stats for the current folder
            # the tuple element at index 8 is the last-modified-date
            stats = os.stat(backup_folder)
            # put the two dates into matching format
            lastmodDate = time.strftime("%m/%d/%y", time.localtime(stats[8]))
            
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


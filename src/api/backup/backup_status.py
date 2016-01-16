'''
Created on Sep 28, 2015

@author: root
'''
from common.enum import Enum

class BackupStatus(Enum):
    '''
    classdocs
    '''
    _status = None
    
    Status = Enum(_status)

    def __init__(self):
        _status = ['process_start', \
                   'backup_start', \
                   'backup_finish', \
                   'backup_fail', \
                   'trans_start', \
                   'trans_finish', \
                   'trans_fail', \
                   'process_fail', \
                   'process_end']
        
        
    
        
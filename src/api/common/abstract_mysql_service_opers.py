#-*- coding: utf-8 -*-
from abc import abstractmethod

'''
Created on 2013-7-21

@author: asus
'''

class Abstract_Mysql_Service_Opers(object):
    
    @abstractmethod
    def start(self):
        raise NotImplementedError, "Cannot call abstract method"
    
    @abstractmethod
    def stop(self):
        raise NotImplementedError, "Cannot call abstract method"
    

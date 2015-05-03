#! /usr/bin/env python
#-*- coding: utf-8 -*-
import os.path
import logging.config
from tornado.options import options

class debug_log():
    
    """
    classdoc
    """
    
    def __init__(self, identifor = 'debug'):
        self.identifor = identifor
        config_path = os.path.join(options.base_dir, "config")
        logging.config.fileConfig(config_path + '/logging.conf')
        
    def get_logger_object(self):
        _logger = logging.getLogger(self.identifor)
        _logger.setLevel(logging.INFO)
        return _logger

    

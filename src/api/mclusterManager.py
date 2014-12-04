#!/usr/bin/env python
#-*- coding: utf-8 -*-

import os.path
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web

import routes
import socket
import logging

import json
from tornado.options import options
from common.appdefine import mclusterManagerDefine
from common.sceduler_opers import Sceduler_Opers
from common.zkOpers import ZkOpers 
from common.configFileOpers import ConfigFileOpers

class Application(tornado.web.Application):
    def __init__(self):
        
        settings = dict(
            template_path=os.path.join(options.base_dir, "templates"),
            ui_modules={"Entry": None},
            xsrf_cookies=False,
            cookie_secret="16oETzKXQAGaYdkL5gEmGeJJFuYh7EQnp5XdTP1o/Vo=",
            login_url="/auth/login",
            debug=options.debug,
        )
        
        tornado.web.Application.__init__(self, routes.handlers, **settings)

def main():
   
#    logging.basicConfig(filename = '/var/log/mcluster-manager/mcluster-manager.log', level = logging.INFO, filemode = 'w', format = '%(asctime)s - %(levelname)s: %(message)s')  
#     if  len(sys.argv) == 2: 
#         if sys.argv[1] == 'debug':
#             _logger = logging.getLogger('root')
#             _logger.setLevel(logging.DEBUG)
#             print 'into debug mode, can see debug contents.'
#         else :
#             logging.info('input correct arguments.')
#             return
#     elif len(sys.argv > 2):
#         logging.info('input arguments too many.')
#         return 
#     else :
#         pass 
    tornado.options.parse_command_line()
    try:
        zk_client = ZkOpers('127.0.0.1', 2181)
        config_file_obj = ConfigFileOpers()
        if (zk_client.existCluster()):
		
       	    clusterUUID = zk_client.getClusterUUID() 
            data, stat = zk_client.retrieveClusterProp(clusterUUID) 

            node_ip_addr = socket.gethostbyname(socket.gethostname())
            return_result = zk_client.retrieve_data_node_info(node_ip_addr)

            if return_result and type(data) is dict:
                config_file_obj.setValue(options.data_node_property, return_result)
                config_file_obj.setValue(options.cluster_property, data) 
            else:
                logging.info("write data into configuration failed")
        else:
             logging.info("Cluster does not exist")
    except Exception, e:
        logging.info("No write ")
        logging.error(e)
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(options.port)
     
    sceduler_opers = Sceduler_Opers()
    
    tornado.ioloop.IOLoop.instance().start()
    
if __name__ == "__main__":
    main()

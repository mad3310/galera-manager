#!/usr/bin/env python
#-*- coding: utf-8 -*-

import os.path
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web

import routes
import logging
import logging.config

from tornado.options import options
from common.appdefine import mclusterManagerDefine
from common.sceduler_opers import Sceduler_Opers

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
    config_path = os.path.join(options.base_dir, "config")
    logging.config.fileConfig(config_path + '/logging.conf')
#    logging.basicConfig(filename = '/var/log/mcluster-manager/mcluster-manager.log', level = logging.INFO, filemode = 'w', format = '%(asctime)s - %(levelname)s: %(message)s')  
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(options.port)
    
    sceduler_opers = Sceduler_Opers()
    
    tornado.ioloop.IOLoop.instance().start()
    
if __name__ == "__main__":
    main()

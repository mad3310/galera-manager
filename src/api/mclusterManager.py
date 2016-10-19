#!/usr/bin/env python
# -*- coding: utf-8 -*-

import tornado.httpserver
import tornado.ioloop
import tornado.web
from tornado.options import options

from common.appdefine import mclusterManagerDefine  #flake8: noqa
from common.scheduler_opers import Scheduler_Opers

import routes


class Application(tornado.web.Application):

    def __init__(self):
        settings = dict(
            debug=options.debug,
        )
        tornado.web.Application.__init__(self, routes.handlers, **settings)


def main():
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(options.port)

    Scheduler_Opers()

    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    main()

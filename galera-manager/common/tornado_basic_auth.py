#!/usr/bin/env python
# -*- coding: utf-8 -*-

# A decorator that lets you require HTTP basic authentication from visitors.
# Kevin Kelley <kelleyk@kelleyk.net> 2011
# Use however makes you happy, but if it breaks, you get to keep both pieces.

# Post with explanation, commentary, etc.:
# http://kelleyk.com/post/7362319243/easy-basic-http-authentication-with-tornado

# Usage example:
#@require_basic_auth
#class MainHandler(tornado.web.RequestHandler):
#    def get(self, basicauth_user, basicauth_pass):
#        self.write('Hi there, {0}!  Your password is {1}.' \
#            .format(basicauth_user, basicauth_pass))
#    def post(self, **kwargs):
#        basicauth_user = kwargs['basicauth_user']
#        basicauth_pass = kwargs['basicauth_pass']
#        self.write('Hi there, {0}!  Your password is {1}.' \
#            .format(basicauth_user, basicauth_pass))

import base64

from tornado.options import options

from configFileOpers import ConfigFileOpers


def check_auth(kwargs):
    s = ConfigFileOpers()
    confDict = s.getValue(options.cluster_property, ['adminUser','adminPassword'])
    targetUserName = confDict['adminUser']
    username = kwargs['basicauth_user']
    if cmp(targetUserName, username) != 0:
        return False

    targetPassword = base64.decodestring(confDict['adminPassword'])
    password = kwargs['basicauth_pass']
    if cmp(targetPassword, password) != 0:
        return False

    return True


def handle_401(handler):
    handler.set_status(401)
    handler.set_header('WWW-Authenticate', 'Basic realm=Restricted')
    handler._transforms = []
    chunk = {}
    chunk['error_code'] = "Authorization failed"
    chunk['errorDetail'] = "Authorization failed"
    handler.finish(chunk)
    return False


def require_basic_auth(handler_class):
    def wrap_execute(handler_execute):
        def require_basic_auth(handler, kwargs):
            auth_header = handler.request.headers.get('Authorization')
            if auth_header is None or not auth_header.startswith('Basic'):
                return handle_401(handler)
            auth_decoded = base64.decodestring(auth_header[6:])
            basicAuthParam = {}
            basicAuthParam['basicauth_user'], basicAuthParam['basicauth_pass'] = auth_decoded.split(':', 2)
            checkResult = check_auth(basicAuthParam)
            if not checkResult:
                return handle_401(handler)
            return True

        def _execute(self, transforms, *args, **kwargs):
            if not require_basic_auth(self, kwargs):
                return False
            return handler_execute(self, transforms, *args, **kwargs)
        return _execute

    handler_class._execute = wrap_execute(handler_class._execute)
    return handler_class

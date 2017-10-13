# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function, with_statement

import sys
import functools

from concurrent.futures import ThreadPoolExecutor

from tornado.gen import Task
from tornado.ioloop import IOLoop
from tornado import stack_context


ERROR_CODE = 500

default_executor = ThreadPoolExecutor(10)


def run_on_executor(executor=default_executor):
    def run_on_executor_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            task = Task(executor.submit, func, *args, **kwargs)
            return task
        return wrapper
    return run_on_executor_decorator


def run_callback(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        callback = kwargs.pop('callback', None)
        assert callback
        try:
            res = func(self, *args, **kwargs)
            callback = stack_context.wrap(callback)
            IOLoop.instance().add_callback(lambda: callback(res))
        except:
            self.write_error(ERROR_CODE, exc_info=sys.exc_info())
    return wrapper

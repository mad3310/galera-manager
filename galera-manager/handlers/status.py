# -*- coding: utf-8 -*-

from tornado.web import asynchronous
from tornado.gen import engine

from common.utils.asyc_utils import run_on_executor, run_callback
from common.consts import MONITOR_TYPE

from base import APIHandler


# retrieve the status value of special monitor type, the monitor type include cluster,node,db.
# In different monitor type, there are many of monitor points.
# eg. curl "http://localhost:8888/mcluster/status/{cluster,node,db}"
class MclusterStatusDetail(APIHandler):

    def get(self, monitor_type):
        if monitor_type == None:
            raise "monitor type should be not null!"

        result = {}

        zkOper = self.retrieve_zkOper()
        monitor_status_list = zkOper.retrieve_monitor_status_list(monitor_type)

        for monitor_status_key in monitor_status_list:
            monitor_status_value = zkOper.retrieve_monitor_status_value(monitor_type, monitor_status_key)
            result.setdefault(monitor_status_key, monitor_status_value)

        self.finish(result)


# retrieve the status value of all monitor type
# eg. curl "http://localhost:8888/mcluster/status"
class MclusterStatus(APIHandler):

    @asynchronous
    @engine
    def get(self):
        result = yield self.do()
        self.finish(result)

    @run_on_executor()
    @run_callback
    def do(self):
        result = {}
        monitor_types = MONITOR_TYPE.keys()
        zkOper = self.retrieve_zkOper()
        for monitor_type in monitor_types:
            monitor_status_list = MONITOR_TYPE.get(monitor_type)
            monitor_type_sub_dict = {}
            for monitor_status_key in monitor_status_list:
                monitor_status_value = zkOper.retrieve_monitor_status_value(monitor_type, monitor_status_key)
                monitor_type_sub_dict.setdefault(monitor_status_key, monitor_status_value)
            result.setdefault(monitor_type, monitor_type_sub_dict)
        return result


class MclusterHealth(APIHandler):

    def get(self):
        result = {"status": "ok"}
        self.finish(result)

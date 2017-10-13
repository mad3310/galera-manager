# -*- coding: utf-8 -*-


import logging
import tornado

from common.status_opers import (Check_Cluster_Available,
                                 Check_Node_Size,
                                 Check_Node_Log_Health,
                                 Check_Node_Log_Error,
                                 Check_Node_Active,
                                 Check_DB_Wsrep_Status,
                                 Check_DB_Cur_Conns,
                                 Check_DB_User_Cur_Conns)
from base import APIHandler


class Cluster_Info_Sync_Handler(object):

    check_cluster_available = Check_Cluster_Available()

    def retrieve_info(self, data_node_info_list):
        return self._action(data_node_info_list)

    def _action(self, data_node_info_list):
        cluster_available_dict = self.check_cluster_available.check(
            data_node_info_list)

        cluster_status_dict = {}
        cluster_status_dict.setdefault(
            "cluster_available", cluster_available_dict)

        return cluster_status_dict


class Cluster_Info_Async_Handler(object):

    def retrieve_info(self, data_node_info_list):
        self._action()

    def _action(self):
        logging.info("_retrieve_cluster_info_async:do nothing!")


class Node_Info_Sync_Handler(object):

    check_node_size = Check_Node_Size()

    def retrieve_info(self, data_node_info_list):
        return self._action(data_node_info_list)

    def _action(self, data_node_info_list):
        node_size_dict = self.check_node_size.check(data_node_info_list)

        node_status_dict = {}
        node_status_dict.setdefault("node_size", node_size_dict)

        return node_status_dict


class Node_Info_Async_Handler(object):

    check_node_log_health = Check_Node_Log_Health()

    check_node_log_error = Check_Node_Log_Error()

    check_node_active = Check_Node_Active()

    def retrieve_info(self, data_node_info_list):
        self._action(data_node_info_list)

    def _action(self, data_node_info_list):
        self.check_node_log_health.check(data_node_info_list)
        self.check_node_log_error.check(data_node_info_list)
        self.check_node_active.check(data_node_info_list)


class DB_Info_Sync_Handler(object):

    def retrieve_info(self, data_node_info_list):
        return self._action(data_node_info_list)

    def _action(self, data_node_info_list):
        logging.info("_retrieve_db_info_sync:do nothing!")


class DB_Info_Async_Handler(object):

    # check_db_wr_available = Check_DB_WR_Avalialbe()

    check_db_wsrep_status = Check_DB_Wsrep_Status()

    check_db_cur_conns = Check_DB_Cur_Conns()

    check_db_user_cur_conns = Check_DB_User_Cur_Conns()

    def retrieve_info(self, data_node_info_list):
        self._action(data_node_info_list)

    def _action(self, data_node_info_list):
        # self.check_db_wr_available.check(data_node_info_list)
        self.check_db_wsrep_status.check(data_node_info_list)
        self.check_db_cur_conns.check(data_node_info_list)
        self.check_db_user_cur_conns.check(data_node_info_list)


# retrieve the status of mcluster
# eg. curl "http://localhost:8888/mcluster/monitor"

class Mcluster_Monitor_Sync(APIHandler):

    cluster_handler = Cluster_Info_Sync_Handler()

    node_handler = Node_Info_Sync_Handler()

    db_handler = DB_Info_Sync_Handler()

    def get(self):
        zkOper = self.retrieve_zkOper()
        data_node_info_list = zkOper.retrieve_data_node_list()

        cluster_status_dict = self.cluster_handler.retrieve_info(
            data_node_info_list)
        node_status_dict = self.node_handler.retrieve_info(data_node_info_list)
        db_status_dict = self.db_handler.retrieve_info(data_node_info_list)

        result = {}
        result.setdefault("cluster", cluster_status_dict)
        result.setdefault("node", node_status_dict)
        self.finish(result)

# retrieve the status of mcluster on background, these status will record into zk
# eg. curl "http://localhost:8888/mcluster/monitor/async"


class Mcluster_Monitor_Async(APIHandler):
    cluster_handler = Cluster_Info_Async_Handler()

    node_handler = Node_Info_Async_Handler()

    db_handler = DB_Info_Async_Handler()

    @tornado.web.asynchronous
    def get(self):
        zkOper = self.retrieve_zkOper()
        data_node_info_list = zkOper.retrieve_data_node_list()

        cluster_status_dict = self.cluster_handler.retrieve_info(
            data_node_info_list)
        node_status_dict = self.node_handler.retrieve_info(data_node_info_list)
        db_status_dict = self.db_handler.retrieve_info(data_node_info_list)

        result = {}
        result.setdefault("message", "finished")
        self.finish(result)

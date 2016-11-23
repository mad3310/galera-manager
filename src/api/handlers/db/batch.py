# -*- coding: utf-8 -*-

import os
import json
import logging

from tornado.gen import engine
from tornado.web import asynchronous, RequestHandler

from common.dba_opers import DBAOpers
from common.utils.asyc_utils import run_on_executor, run_callback
from common.zkOpers import Requests_ZkOpers
from controllers.db.batch import SQLBatch
from controllers.db.consts import DDL_TYPE

DIR_MCLUSTER_MYSQ = '/srv/mcluster/mysql'


class DDLBatch(RequestHandler):

    """/db/{db_name}/ddl/batch
    """

    def initialize(self):
        self.zk = Requests_ZkOpers()

    @asynchronous
    @engine
    def post(self, db_name):
        body = json.loads(self.request.body, strict=False, encoding='utf-8')
        pts = body.get('pts', '[]')
        if not pts:
            self.set_status(400)
            self.finish({"errmsg": "required argument is empty", "errcode": 40001})
            return

        # 写开始状态到zk
        ddl_status = dict(isFinished=False, status='sql batch ddl is in processing')
        yield self.set_start_status(db_name, ddl_status)
        self.finish(ddl_status)

        yield self.ddl_execute(db_name, pts)

    @run_on_executor()
    @run_callback
    def set_start_status(self, db_name, ddl_status):
        self.zk.write_sqlbatch_ddl_info(db_name, ddl_status)

    @run_on_executor()
    @run_callback
    def ddl_execute(self, db_name, pts):
        batch = SQLBatch(db_name)
        is_finished = True
        ddl_status = {}
        for pt in pts:
            ddl_sqls = pt.get("ddlSqls")
            tb_name = pt.get("tbName")
            ddl_type = pt.get('type', 'ALTER')
            # DDL语句：ALTER类型走PT工具执行，其他语句直接调用Mysql客户端执行
            if ddl_type == DDL_TYPE.ALTER:
                ret = self.execute_one(batch, db_name, tb_name, ddl_sqls)
            else:
                ret = self.execute_direct(batch, db_name, ddl_sqls)
            if not ret:
                is_finished = False
                ddl_status = dict(isFinished=is_finished,
                                  status='sql batch ddl({0}) is failed'.format(ddl_sqls))
                break
        if is_finished:
            ddl_status = dict(isFinished=is_finished, status='sql batch ddl is successed')
        self.zk.write_sqlbatch_ddl_info(db_name, ddl_status)

    def execute_direct(self, batch, db_name, sql):
        ret = True
        ddl_status = dict(isFinished=False,
                          status='sql batch ddl({0}) is in processing'.format(sql))
        error = batch.sql_excute(sql)
        logging.info("[DDL Batch] execute_direct sqls: {0}".format(sql))
        if error:
            ddl_status.update(dict(status=error))
            ret = False
        self.zk.write_sqlbatch_ddl_info(db_name, ddl_status)
        return ret

    def execute_one(self, batch, db_name, tb_name, ddl_sqls):
        # PT-OSC工具执行分两步操作：
        # 先测试，成功返回：Dry run complete.
        ddl_status = dict(isFinished=False,
                          status='sql batch ddl ({0}) is failed'.format(ddl_sqls))

        ret = batch.ddl_test(ddl_sqls, tb_name)
        logging.info("[DDL Batch] test sqls: {0}".format(ddl_sqls))
        if not ret:
            logging.error("[DDL Batch] test error: {0}".format(ddl_sqls))
            self.zk.write_sqlbatch_ddl_info(db_name, ddl_status)
            return False

        # 再执行，成功返回：Successfully.
        ret = batch.ddl(ddl_sqls, tb_name)
        logging.info("[DDL Batch] result sqls: {0}".format(ddl_sqls))
        if not ret:
            logging.error("[DDL Batch] error: {0}".format(ddl_sqls))
            self.zk.write_sqlbatch_ddl_info(db_name, ddl_status)
            return False
        return True


class DDLBatchCheck(RequestHandler):

    """/db/{db_name}/ddl/batch/check
    """

    @asynchronous
    @engine
    def get(self, db_name):
        ddl_status = yield self.get_status(db_name)
        self.finish(ddl_status)

    @run_on_executor()
    @run_callback
    def get_status(self, db_name):
        zk = Requests_ZkOpers()
        ddl_status = zk.retrieve_sqlbatch_ddl_status_info(db_name)
        return ddl_status


class DMLBatch(RequestHandler):

    """/db/{db_name}/dml/batch
    """

    def initialize(self):
        self.zk = Requests_ZkOpers()

    @asynchronous
    @engine
    def post(self, db_name):
        body = json.loads(self.request.body, strict=False, encoding='utf-8')
        dml_sqls = body.get("dmlSqls")
        if not dml_sqls:
            self.set_status(400)
            self.finish({"errmsg": "required argument is none", "errcode": 40001})
            return

        # 写开始状态到zk
        dml_status = dict(isFinished=False, status='sql batch dml is in processing')
        yield self.set_start_status(db_name, dml_status)
        self.finish(dml_status)

        yield self.dml_execute(db_name, dml_sqls)

    @run_on_executor()
    @run_callback
    def set_start_status(self, db_name, dml_status):
        self.zk.write_sqlbatch_dml_info(db_name, dml_status)

    @run_on_executor()
    @run_callback
    def dml_execute(self, db_name, dml_sqls):
        sqls = dml_sqls.split(";")
        sqls = [sql for sql in sqls if sql]

        batch = SQLBatch(db_name)
        error = batch.dml(sqls)

        # 写结束状态到zk
        is_finished = False if error else True
        status = error or 'sql batch dml is successed'
        dml_status = dict(isFinished=is_finished, status=status)
        self.zk.write_sqlbatch_dml_info(db_name, dml_status)


class DMLBatchCheck(RequestHandler):

    """/db/{db_name}/dml/batch/check
    """

    @asynchronous
    @engine
    def get(self, db_name):
        dml_status = yield self.get_status(db_name)
        self.finish(dml_status)

    @run_on_executor()
    @run_callback
    def get_status(self, db_name):
        zk = Requests_ZkOpers()
        dml_status = zk.retrieve_sqlbatch_dml_status_info(db_name)
        return dml_status


class TablesRows(RequestHandler):

    """/db/{db_name}/tables/rows
    """

    def post(self, db_name):
        body = json.loads(self.request.body, strict=False, encoding='utf-8')
        tables = body.get("tables")

        # 判断数据库是否存在
        db_dir = DIR_MCLUSTER_MYSQ + '/{db_name}'.format(db_name=db_name)
        if not os.path.exists(db_dir):
            self.set_status(400)
            self.finish({"errmsg": "db is not exist", "errcode": 40031})
            return

        result = DBAOpers.get_tables_rows(db_name, tables)

        # 判断是否有不存在的表
        for tb, row in result.items():
            if not row:
                self.set_status(400)
                result = {"errmsg": "table {0} is not exist".format(tb), "errcode": 40401}
                break
        self.finish(result)

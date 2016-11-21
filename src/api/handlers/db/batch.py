# -*- coding: utf-8 -*-

import json
import logging

from tornado.gen import engine
from tornado.web import asynchronous, RequestHandler

from common.dba_opers import DBAOpers
from common.utils.asyc_utils import run_on_executor, run_callback
from controllers.db.batch import SQLBatch
from common.zkOpers import Requests_ZkOpers


class DDLBatch(RequestHandler):

    """/db/{db_name}/ddl/batch
    """
    @asynchronous
    @engine
    def post(self, db_name):
        body = json.loads(self.request.body, strict=False,
                          encoding='utf-8')
        pts = json.loads(body.get('pts', '[]'))
        self.zk = Requests_ZkOpers()
        self.batch = SQLBatch(db_name)
        if not pts:
            self.set_status(400)
            self.finish({"status": "required argument is empty", "errcode": 40001})
            return
        yield self.set_ddl_begin(db_name)
        self.finish({"status": 'sql batch ddl is in processing'})
        yield self.ddl_execute(db_name, pts)

    @run_on_executor()
    @run_callback
    def set_ddl_begin(self, db_name):
        ddl_status = dict(isFinished=False,
                          status='sql batch ddl is in processing')
        self.zk.write_sqlbatch_ddl_info(db_name, ddl_status)

    @run_on_executor()
    @run_callback
    def ddl_execute(self, db_name, pts):
        is_finished = True
        for pt in pts:
            ddl_sqls = pt.get("ddlSqls")
            tb_name = pt.get("tbName")
            ddl_type = pt.get('type', 'ALTER')
            if ddl_type == 'ALTER':
                ret = self.ddl_execute_one(ddl_sqls, db_name, tb_name)
            else:
                ret = self.ddl_execute_direct(db_name, ddl_sqls)
            if not ret:
                is_finished = False
                break
        if is_finished:
            ddl_status = dict(is_finished=True,
                              status='sql batch ddl successed')
            self.zk.write_sqlbatch_ddl_info(db_name, ddl_status)

    def ddl_execute_direct(self, db_name, sql):
        batch = self.batch
        ret = True
        ddl_status = dict(isFinished=False,
                          status='batch ddl sql(%s) is in processing' % sql)
        error = batch.sql_excute(sql)
        if error:
            ddl_status.update(dict(status=error, errcode=40005))
            ret = False
        self.zk.write_sqlbatch_ddl_info(db_name, ddl_status)
        return ret

    def ddl_execute_one(self, ddl_sqls, db_name, tb_name):
        """
        PT-OSC工具执行分两步操作：
        先测试，成功返回：
        Dry run complete.
        """
        batch = self.batch
        ret = batch.ddl_test(ddl_sqls, tb_name)
        logging.info("[DDL Batch] test sqls: {0}".format(ddl_sqls))
        ddl_status = dict(isFinished=False,
                          status='%s sql batch ddl is in processing' % tb_name)
        if not ret:
            error = 'alter table %s %s failed' % (tb_name, ddl_sqls)
            logging.error("[DDL Batch] test error: {0}".format(error))
            # self.set_status(400)
            # self.finish({"status": ret, "errcode": 40005})
            ddl_status.update(dict(status=error,
                              errcode=40005))
            self.zk.write_sqlbatch_ddl_info(db_name, ddl_status)
            return False

        """
        再执行，成功返回：
        Successfully.
        """
        ret = batch.ddl(ddl_sqls, tb_name)
        logging.info("[DDL Batch] result sqls: {0}".format(ddl_sqls))
        if not ret:
            error = 'alter table %s %s failed' % (tb_name, ddl_sqls)
            logging.error("[DDL Batch] error: {0}".format(error))
            # self.set_status(400)
            # self.finish({"status": ret, "errcode": 40005})
            ddl_status.update(dict(status=error,
                              errcode=40005))
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
        if 'errcode' in ddl_status:
            self.set_status(400)
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
    @asynchronous
    @engine
    def post(self, db_name):
        body = json.loads(self.request.body, strict=False,
                          encoding='utf-8')
        dml_sqls = body.get("dmlSqls")
        self.zk = Requests_ZkOpers()
        if not dml_sqls:
            self.set_status(400)
            self.finish({"status": "required argument is none", "errcode": 40001})
            return
        yield self.set_dml_begin(db_name)
        self.finish({"status": 'sql batch dml is in processing'})
        yield self.sqlbatch_dml_execute(db_name, dml_sqls)

    @run_on_executor()
    @run_callback
    def set_dml_begin(self, db_name):
        dml_status = dict(isFinished=False,
                          status='sql batch dml is in processing',
                          )
        self.zk.write_sqlbatch_dml_info(db_name, dml_status)

    @run_on_executor()
    @run_callback
    def sqlbatch_dml_execute(self, db_name, dml_sqls):
        sqls = dml_sqls.split(";")
        sqls = [sql for sql in sqls if sql]
        dml_status = dict(isFinished=False,
                          status='sql batch dml is in processing',
                          )
        batch = SQLBatch(db_name)

        error = batch.dml(sqls)
        if error:
            dml_status.update(dict(errcode=400))
        is_finished = True if (not error) else False
        status = error or 'sql batch dml successed'
        dml_status.update(dict(isFinished=is_finished,
                               status=status))
        # self.finish({"status": result})
        self.zk.write_sqlbatch_dml_info(db_name, dml_status)


class DMLBatchCheck(RequestHandler):

    """/db/{db_name}/dml/batch/check
    """
    @asynchronous
    @engine
    def get(self, db_name):
        dml_status = yield self.get_status(db_name)
        if 'errcode' in dml_status:
            self.set_status(400)
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
    @asynchronous
    @engine
    def post(self, db_name):
        body = json.loads(self.request.body, strict=False,
                          encoding='utf-8')
        tables = json.loads(body.get("tables"))
        result = yield self.get_rows(db_name, tables)
        ret = {}
        # 判断是否有不存在的表
        for tb, size in result.items():
            if not size:
                self.set_status(400)
                ret = {"status": "table {0} is not exist".format(tb), "errcode": 404001}
                break
                # self.finish({"status": "table {0} is not exist".format(tb), "errcode": 404001})
            else:
                ret[tb] = str(size)
        self.finish(ret)

    @run_on_executor()
    @run_callback
    def get_rows(self, db_name, tables):
        db = DBAOpers()
        return db.get_tables_rows(db_name, tables)

# -*- coding: utf-8 -*-

import os
import json
import logging

from tornado.gen import engine
from tornado.web import asynchronous
from tornado.web import RequestHandler

from common.dba_opers import DBAOpers
from common.utils import disk_available, dir_size
from common.utils.asyc_utils import run_on_executor, run_callback
from controllers.dump.dump import Dump
from controllers.dump.consts import DIR_MCLUSTER_MYSQ


class DBDump(RequestHandler):

    """/db/dump
    """

    @asynchronous
    @engine
    def post(self, db_name):
        """dump条件：数据库大小小于磁盘剩余空间"""
        body = json.loads(self.request.body, encoding='utf-8')
        user_id = body.get("userId")
        db_id = body.get("dbId")
        tb_name = body.get("tbName")
        if not user_id or not db_id or not db_name:
            self.set_status(400)
            self.finish({"errmsg": "required argument is none", "errcode": 40001})
            return

        if not is_disk_space_enough(db_name, tb_name=tb_name):
            self.set_status(400)
            self.finish({"errmsg": "disk space is not enough", "errcode": 40030})
            return

        dump = Dump(user_id, db_id)
        file_name = dump.generate_file_name(db_name, tb_name=tb_name)
        self.finish({"fileName": file_name})

        logging.info("[dump] db {0} start".format(db_name))
        yield self.dump_exector(dump, db_name, tb_name=tb_name)

    @run_on_executor()
    @run_callback
    def dump_exector(self, dump, db_name, tb_name=None):
        dump.execute(db_name, tb_name=tb_name)
        logging.info("[dump] execute succseeful")


class DumpCheck(RequestHandler):

    """ /db/dump/check
    """

    def get(self, file_name):
        r = Dump.is_upload_s3(file_name)
        if not r:
            self.set_status(404)
            self.finish({"errmsg": "dump file is not uploaded", "errcode": 40012})
            return

        url = Dump.url(file_name)

        result = {"url": url}
        self.finish(result)


def is_disk_space_enough(db_name, tb_name=None):
    """探测磁盘空间是否够用"""
    available = disk_available(DIR_MCLUSTER_MYSQ) if os.path.exists(DIR_MCLUSTER_MYSQ) else 0
    db_dir = DIR_MCLUSTER_MYSQ + '/{db_name}'.format(db_name=db_name)
    db_size = dir_size(db_dir)
    if tb_name:
        db_size = DBAOpers.get_table_size(db_name, tb_name)
    logging.info("[dump] space available:{0},db_size:{1}".format(available, db_size))
    return available > db_size

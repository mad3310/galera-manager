# -*- coding: utf-8 -*-

import json

from tornado.gen import engine
from tornado.web import asynchronous

from common.utils.exceptions import HTTPAPIErrorException
from common.utils import disk_available, dir_size
from common.dba_opers import DBAOpers
from common.utils.asyc_utils import run_on_executor
from controllers.dump.dump import Dump
from controllers.dump.consts import DIR_MCLUSTER_MYSQ

from base import APIHandler


class DBDump(APIHandler):

    """/db/dump
    """

    @asynchronous
    @engine
    def post(self):
        """dump条件：数据库大小小于磁盘剩余空间"""
        body = json.loads(self.request.body, encoding='utf-8')
        user_id = body.get("userId")
        db_id = body.get("dbId")
        db_name = body.get("dbName")
        tb_name = body.get("tbName")
        if not user_id or not db_id or not db_name:
            raise HTTPAPIErrorException("arguments is none", status_code=400)

        if not is_disk_space_enough(db_name, tb_name=tb_name):
            raise HTTPAPIErrorException("disk space is not enough", status_code=404)

        dump = Dump(user_id, db_id)
        file_name = dump.generate_file_name(db_name, tb_name=tb_name)
        self.finish({"fileName": file_name})

        yield self.dump_exector(dump, db_name, tb_name=tb_name)

    @run_on_executor()
    def dump_exector(self, dump, db_name, tb_name=None):
        dump.execute(db_name, tb_name=tb_name)


class DumpCheck(APIHandler):

    """ /db/dump/check
    """

    def get(self):
        file_name = self.get_argument("fileName")
        if not file_name:
            raise HTTPAPIErrorException("file name is none", status_code=400)

        r = Dump.is_upload_s3(file_name)
        if not r:
            raise HTTPAPIErrorException("dump file is not uploaded", status_code=404)

        url = Dump.url(file_name)
        result = {"url": url}
        self.finish(result)


def is_disk_space_enough(db_name, table_name=None):
    """探测/srv/mcluster/mysql/db_name大小"""
    available = disk_available(DIR_MCLUSTER_MYSQ)
    db_dir = DIR_MCLUSTER_MYSQ + '/{db_name}'.format(db_name=db_name)
    db_size = dir_size(db_dir)
    if table_name:
        db_size = DBAOpers.get_table_size(db_name, table_name)
    return available > db_size

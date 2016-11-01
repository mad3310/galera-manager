# -*- coding: utf-8 -*-

from base import APIHandler
from common.utils.exceptions import HTTPAPIErrorException
from common.utils import disk_available, dir_size
from common.dba_opers import DBAOpers
from common.consts import MCLUSTER_MYSQL_DIR
from controllers.dump.dump import Dump


class DBDump(APIHandler):

    # /db/dump

    def post(self, db_name):
        """dump条件：数据库大小小于磁盘剩余空间"""
        if not db_name:
            raise HTTPAPIErrorException("db_name is none", status_code=400)

        db_dir = MCLUSTER_MYSQL_DIR + '/{db_name}'.format(db_name=db_name)
        size = dir_size(db_dir)
        available = disk_available(MCLUSTER_MYSQL_DIR)
        if available < size:
            raise HTTPAPIErrorException("disk space is not enough", status_code=401)

        r = Dump.db_dump(db_name)

        result = {"message": r}
        self.finish(result)


class TableDump(APIHandler):

    # /db/dump/table

    def post(self, db_name, table_name):
        """dump条件：数据表大小小于磁盘剩余空间"""
        if not db_name:
            raise HTTPAPIErrorException("db_name is none", status_code=400)
        if not table_name:
            raise HTTPAPIErrorException("table_name is none", status_code=400)

        size = DBAOpers.get_table_size(db_name, table_name)
        available = disk_available(MCLUSTER_MYSQL_DIR)
        if available < size:
            raise HTTPAPIErrorException("disk space is not enough", status_code=401)

        r = Dump.table_dump(db_name, table_name)

        result = {"message": r}
        self.finish(result)


class DumpCheck(APIHandler):

    # /db/dump/check

    def get(self):
        result = {"status": "ok"}
        self.finish(result)

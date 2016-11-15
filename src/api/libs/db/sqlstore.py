# -*- coding: utf-8 -*-

"""
    mysql store
"""

import sys
import threading
import MySQLdb


class SqlStore(object):

    def __init__(self, context, retry=1):
        self.retry = retry
        self.lock = threading.Lock()
        self.context = context
        self.executed_sql = []

    def __repr__(self):
        return '<SqlStore user:{0}>'.format(self.context.config.get('DB_USER'))

    def _conn(self):
        db_host = self.context.config.get('DB_HOST')
        db_port = self.context.config.get('DB_PORT')
        db_user = self.context.config.get('DB_USER')
        db_pwd = self.context.config.get('DB_PASSWD')
        db_name = self.context.config.get('DB_NAME')
        return MySQLdb.connect(host=db_host,
                               port=db_port,
                               user=db_user,
                               passwd=db_pwd,
                               db=db_name,
                               use_unicode=True,
                               charset="utf8")

    def get_cursor(self):
        return self._conn().cursor()

    cursor = property(get_cursor,)

    #flake8: noqa
    def execute_retry(func):
        def call(self, *args, **kwargs):
            attempts = 0
            while True:
                self.lock.acquire()
                try:
                    return func(self, *args, **kwargs)
                except MySQLdb.Error as e:
                    if attempts >= self.retry:
                        raise
                    # (2006, 'MySQL server has gone away')
                    if e.args[0] == 2006:
                        self._conn.close()
                        del self._conn
                    attempts += 1
                finally:
                    self.lock.release()
        return call

    @execute_retry
    def execute(self, sql, args=None):
        if args is not None and not isinstance(args, (list, tuple, dict)):
            args = (args,)

        for retry in xrange(self.retry, -1, -1):
            try:
                cursor = self._conn().cursor()
                if (sql, args,) not in self.executed_sql:
                    cursor.execute(sql, args)
                    self.executed_sql.append((sql, args))
            except MySQLdb.OperationalError:
                exc_class, exception, tb = sys.exc_info()
                if not retry:
                    raise exc_class, exception, tb
        return cursor

    def commit(self):
        r = self._conn().commit()
        for sql, args in self.executed_sql:
            self.executed_sql = []
        return r

    def rollback(self):
        r = self._conn().rollback()
        self.executed_sql = []
        return r

    @classmethod
    def init_by_context(cls, context):
        return SqlStore(context)

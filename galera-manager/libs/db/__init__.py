# -*- coding: utf-8 -*-

"""
    mysql store
"""

from tornado.options import options


def _get_db(db_name):
    if not getattr(_get_db, '_store', None):
        from .context import init_context
        from .sqlstore import SqlStore
        context = init_context(options.MYSQL_DSN, db_name)
        conn = SqlStore(context, retry=2)
        _get_db._store = conn
    return _get_db._store

db = _get_db

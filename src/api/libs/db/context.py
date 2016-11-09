# -*- coding: utf-8 -*-

from urlparse import urlparse


class SQLContext(object):

    def __init__(self, **config):
        assert config.get('DB_HOST')
        assert config.get('DB_PORT')
        assert config.get('DB_USER')
        assert config.get('DB_NAME')
        self.config = config


def init_context(dsn, db_name, db_user=None, db_pwd=None):
    develop_mode = True
    netloc = urlparse(dsn).netloc.split(':')
    host = netloc[0]
    assert host
    port = int(netloc[1]) or 3306

    return SQLContext(DEVELOP_MODE=develop_mode,
                      DB_HOST=host,
                      DB_PORT=port,
                      DB_USER=db_user or 'root',
                      DB_PASSWD=db_pwd or 'Mcluster',
                      DB_NAME=db_name)

# -*- coding: utf-8 -*-

import os
import logging
import subprocess

from libs.fs.s3 import s3

from .consts import (DIR_MCLUSTER, S3_DUMP_FILE_PATH,
                     DUMP_DB_COMMAND, DUMP_TABLE_COMMAND)


class Dump(object):
    def __init__(self, user_id, db_id):
        self.user_id = user_id
        self.db_id = db_id

    def __repr__(self):
        return '<Dump user:{0} db:{1}>'.format(self.user_id, self.db_id)

    def execute(self, db_name, tb_name=None):
        file_name = self.generate_file_name(db_name, tb_name=tb_name)
        local_path = self.get_local_path(file_name)
        s3_path = self.get_s3_path(file_name)

        # 先删除已存在的文件
        self.delete_s3(s3_path)
        self.delete_local(local_path)

        # 再执行dump
        if tb_name:
            command = DUMP_TABLE_COMMAND.format(db_name=db_name,
                                                tb_name=tb_name,
                                                local_path=local_path)
        else:
            command = DUMP_DB_COMMAND.format(db_name=db_name,
                                             local_path=local_path)
        logging.info("[dump] command:{0}".format(command))

        p = subprocess.Popen(command, shell=True,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)
        p.wait()

        # 上传到S3
        if os.path.exists(local_path):
            s3.upload_file(local_path, s3_path)
            logging.info("[dump] upload file [{0}] to [matrix/{1}]".format(local_path, s3_path))

        # 再次删除本地dump文件
        self.delete_local(local_path)
        return file_name

    def generate_file_name(self, db_name, tb_name=None):
        """
        文件名称格式：
        库：userid_dbid_dbname.sql
        表：userid_dbid_dbname_tbname.sql
        """
        return ("{0}_{1}_{2}_{3}.sql".format(self.user_id, self.db_id, db_name, tb_name) if
                tb_name else "{0}_{1}_{2}.sql".format(self.user_id, self.db_id, db_name))

    @classmethod
    def url(cls, file_name):
        key = cls.get_s3_path(file_name)
        return s3.get_url(key)

    @classmethod
    def is_upload_s3(cls, file_name):
        """是否上传到S3"""
        key = cls.get_s3_path(file_name)
        return s3.is_file_exists(key)

    @classmethod
    def get_local_path(cls, file_name):
        """本地路径
        /srv/mcluster/userid_dbid_dbname.sql
        """
        return "/".join((DIR_MCLUSTER, file_name))

    @classmethod
    def get_s3_path(cls, file_name):
        """S3上路径
        mcluster/dump/userid_dbid_dbname.sql
        """
        return "/".join((S3_DUMP_FILE_PATH, file_name))

    def delete_local(self, path):
        """删除本地"""
        if os.path.exists(path):
            os.remove(path)
            logging.info("[dump] delete local file:{0}".format(path))

    def delete_s3(self, path):
        """删除远端"""
        if s3.is_file_exists(path):
            s3.delete_file(path)
            logging.info("[dump] delete s3 file:{0}".format(path))

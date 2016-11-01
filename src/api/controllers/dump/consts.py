# -*- coding: utf-8 -*-

# dump文件上传到S3的路径
S3_DUMP_FILE_PATH = 'mcluster/dump'

# 本地DB实例文件夹
DIR_MCLUSTER = '/srv/mcluster'
DIR_MCLUSTER_MYSQ = '/srv/mcluster/mysql'

DUMP_DB_COMMAND = ('mysqldump -uroot -pMcluster -hlocalhost --opt --single-transaction '
                   '--default-character-set=utf8 {db_name} > {local_path}')

DUMP_TABLE_COMMAND = ('mysqldump -uroot -pMcluster -hlocalhost --opt --single-transaction '
                      '--default-character-set=utf8 {db_name} {tb_name} > {local_path}')

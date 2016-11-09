# -*- coding: utf-8 -*-

from utils.storify import storify

BACKUP_TYPE = storify(dict(
    FULL='full',
    INCR='incr',
))

BACKUP_SECRET = storify(dict(
    USER='backup',
    PASSWD='backup',
))

LOCAL = '/srv/mcluster/mcluster_data/hot_backup/xbstream/rs/full_add'  # 备份的本地目录
REMOTE = '/data/mcluster_data/hot_backup/xbstream/rs/full_add'         # 备份的远程目录

BACKUP_CONFIG = storify(dict(
    LOG_FILE_PATH='backup',
    FULL_LOCAL_DIR=LOCAL,
    FULL_REMOTE_DIR=REMOTE,
    INCR_LOCAL_DIR='%s/incr' % LOCAL,
    INCR_REMOTE_DIR='%s/incr' % REMOTE,
    TMP_DIR="/tmp/incr",
    TMP_FILE="/tmp/incr/innobackupex-tmp-"
))

#!/usr/bin/env python2.6
#-*- coding: utf-8 -*-

BACKUP_USER = 'backup'  
BACKUP_PASSWD = 'backup'
LOG_FILE_PATH = '/var/log/mcluster-manager/mcluster-backup' 


BACKUPDIR='/srv/mcluster/mcluster_data/hot_backup/xbstream/rs/full_add' # 备份的本地目录
REMOTE_BACKUPDIR='/data/mcluster_data/hot_backup/xbstream/rs/full_add' 

INCRBACKUPDIR='%s/incr' %BACKUPDIR # 增量备份的目录
REMOTE_INCRBACKUPDIR='%s/incr' %REMOTE_BACKUPDIR # 远程增量备份的目录

TMPFILEDIR="/tmp/incr"
TMPFILE="%s/innobackupex-tmp-" %TMPFILEDIR
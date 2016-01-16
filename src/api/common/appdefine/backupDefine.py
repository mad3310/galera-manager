#!/usr/bin/env python2.6
#-*- coding: utf-8 -*-

BACKUP_USER = 'backup'  
BACKUP_PASSWD = 'backup'
LOG_FILE_PATH = '/var/log/mcluster-manager/mcluster-backup/' 

BACKUP_PATH = "/srv/mcluster/mcluster_data/hot_backup/xbstream/rs/"
REMOTE_BACKUP_PATH='/data/mcluster_data/hot_backup/xbstream/rs/'

BACKUPDIR='/srv/mcluster/mcluster_data/hot_backup/xbstream/rs/full_add/' # 备份的本地目录
REMOTE_BACKUPDIR='/data/mcluster_data/hot_backup/xbstream/rs/full_add/' 

INCRBACKUPDIR='%sincr' %BACKUP_PATH # 增量备份的目录
REMOTE_INCRBACKUPDIR='%sincr' %REMOTE_BACKUP_PATH # 远程增量备份的目录

TMPFILE="/tmp/innobackupex-runner.$$.tmp"
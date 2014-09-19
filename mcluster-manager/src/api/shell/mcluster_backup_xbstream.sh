#!/bin/bash

BACKUP_USER=backup
BACKUP_PASSWD=backup
FULL_RS_PATH=/srv/mcluster/mcluster_data/hot_backup/xbstream/rs/full
REMOTE_FULL_RS_PATH=/data/mcluster_data/hot_backup/xbstream/rs/full
LOG_PATH=/var/log/mcluster-manager/
LOG_FILE=/var/log/mcluster-manager/mcluster-backup.log
MY_CNF_PATH=/opt/letv/mcluster/root/etc/my.cnf
BACKUP_TMP_PATH=/srv/mcluster/mcluster_data/hot_backup
INNOBACKUPEX=innobackupex
date=`date +%Y%m%d%H%M`
yesterday_date=`date --date="1440 minutes ago" +%Y%m%d%H%M`
yesterday_file_name=full_backup_${yesterday_date}.xbstream

for i in $FULL_RS_PATH $LOG_PATH $REMOTE_FULL_RS_PATH
do
 [ ! -d $i ] && mkdir -p $i
done

if [ -e $FULL_RS_PATH/full_backup.xbstream ]; then
   mv $FULL_RS_PATH/full_backup.xbstream $FULL_RS_PATH/${yesterday_file_name}
fi

echo -e  "\n === Backup All Data! === \n `date +%Y.%m.%d\ %R:%S\ start...`" >> $LOG_FILE
$INNOBACKUPEX --user=$BACKUP_USER --password=$BACKUP_PASSWD --defaults-file=$MY_CNF_PATH --compress --compress-threads=8 --parallel=4 --stream=xbstream   $BACKUP_TMP_PATH > $FULL_RS_PATH/full_backup.xbstream
if [ $? -nt 0 ]; then 
	echo "backup failed,please check..." >> $LOG_FILE | mail -s "$HOSTNAME\'s backup job error" zhoubingzheng@letv.com 
fi

## move yesterday backup file to remote file system
mv $FULL_RS_PATH/${yesterday_file_name} $REMOTE_FULL_RS_PATH/

## delete old files
find $REMOTE_FULL_RS_PATH -type f -mtime +7 -exec rm {} \;

echo  `date +%Y.%m.%d\ %R:%S\ end!!` >> $LOG_FILE

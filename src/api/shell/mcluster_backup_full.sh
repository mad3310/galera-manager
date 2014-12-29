#!/bin/bash
BACKUP_USER=backup
BACKUP_PASSWD=backup
BACKUP_RS_PATH=/srv/mcluster/mcluster_data/hot_backup/xbstream/rs/full_add
REMOTE_BACKUP_RS_PATH=/data/mcluster_data/hot_backup/xbstream/rs/full_add
LOG_FILE_PATH=/var/log/mcluster-manager/mcluster-backup/
LOG_FILE_SCRIPTE=/var/log/mcluster-manager/mcluster-backup/`date +%Y%m%d%H%M%S`_script.log
MY_CNF_PATH=/opt/letv/mcluster/root/etc/my.cnf
INNOBACKUPEX=innobackupex
date_suffix=`date +%Y%m%d%H%M%S`
MCLUSTER_LIST=$((`df -h|awk '/\/srv\/mcluster/{for(i=1;i<=NF;i++)if($i ~ /\/srv\/mcluster/) print i}'` - 2))
MCLUSTER_DISK_FREE=`df -m|awk "/\/srv\/mcluster/{print $"$MCLUSTER_LIST"}"`
DATA_LIST=$((`df -h|awk '/\/data/{for(i=1;i<=NF;i++)if($i ~ /\/data/) print i}'` - 2))
DATA_DISK_FREE=`df -m|awk "/\/data/{print $"$DATA_LIST"}"`
MCLUSTER_USAGE=$((`du -sm /srv/mcluster/mysql|awk  '{print $1}'` + `du -sm /srv/mcluster/mysql|awk  '{print $1}'`))
for i in $BACKUP_RS_PATH $LOG_FILE_PATH  $REMOTE_BACKUP_RS_PATH;
do
   [ ! -d $i ] && mkdir -p $i
done

fb_update_index(){
   rm -rf $REMOTE_BACKUP_RS_PATH/full
   ln -s $1 $REMOTE_BACKUP_RS_PATH/full
}

log(){
        msg=$1
        date_str=$(date +%Y.%m.%d" "%R:$S" ")
        echo "$date_str $msg" >> $LOG_FILE_SCRIPTE
}

log_failed(){
        msg=$1
        log "== script is failed == : $msg"
}

fb_file_lock(){
   if [ -f /var/lock/backup.lock  ];then
      echo "xtrabackup is running"
      exit 1
   fi
   touch  /var/lock/backup.lock
}

fb_file_unlock(){
   rm -f /var/lock/backup.lock
}

fail_exit(){
   rm -f /var/lock/backup.lock
   exit 1
}

full_backup(){
   log "== Mysql backup  is starting  =="
   if [ `ss -tlnp|grep 3306|wc -l` -eq  1  -a  -f /opt/letv/mcluster/root/var/run/mysqld/mysqld.pid   ];then
      if [ $MCLUSTER_DISK_FREE -ge $MCLUSTER_USAGE -a $DATA_DISK_FREE -ge $MCLUSTER_USAGE ];then
         $INNOBACKUPEX --user=$BACKUP_USER --password=$BACKUP_PASSWD --defaults-file=$MY_CNF_PATH --no-timestamp $BACKUP_RS_PATH/full_backup-$date_suffix  >>$LOG_FILE_PATH/`date +%Y%m%d%H%M%S`_backup.log 2>&1
         if [ $? -eq 0 ];then
            log "== Backup All Data end =="
         else
            log_failed "Backup All Data is ERROR"
            fail_exit
         fi
         cp -a $BACKUP_RS_PATH/full_backup-$date_suffix $REMOTE_BACKUP_RS_PATH/
         if [ $? -eq 0 ];then
            log "== Cp backup_file ok  =="
         else
            log_failed "Cp backup_file is  ERROR"
            fail_exit
         fi
         fb_update_index $REMOTE_BACKUP_RS_PATH/full_backup-$date_suffix
         rm -rf $BACKUP_RS_PATH/full_backup-$date_suffix
         find $LOG_FILE_PATH -type f -atime +10 -name "*.log" -exec rm -f {} \; 
         log "== the script is ok =="
      else
         log_failed "The disk is full"
         fail_exit
      fi
   else
         log_failed "Mcluster is not start"
         fail_exit
   fi
}

echo $LOG_FILE_SCRIPTE
fb_file_lock
full_backup
fb_file_unlock

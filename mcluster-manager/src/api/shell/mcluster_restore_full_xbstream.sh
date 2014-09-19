#!/bin/bash

FULL_RS_PATH=/srv/mcluster/mcluster_data/hot_backup/xbstream/rs/full
FULL_RESTORE_TEMP_PATH=/srv/mcluster/mcluster_data/restore/full/mysql
LOG_PATH=/var/log/mcluster-manager/
LOG_FILE=/var/log/mcluster-manager/mcluster-restore.log
MY_CNF_PATH=/opt/letv/mcluster/root/etc/my.cnf
MYSQL_DATA_FILE_PATH=/srv/mcluster/mysql
MYSQL_DATA_PATH=/srv/mcluster
INNOBACKUPEX=innobackupex
QPRESS=/usr/local/qpress/qpress
XBSTREAM=xbstream
date=`date +%Y%m%d%H%M`

for i in $FULL_RS_PATH $LOG_PATH $FULL_RESTORE_TEMP_PATH
do
 [ ! -d $i ] && mkdir -p $i
done

echo -e  "\n === Restore Processing! === \n `date +%Y.%m.%d\ %R:%S\ start...`" >> $LOG_FILE

echo -e  "\n === XBstream--> File === \n `date +%Y.%m.%d\ %R:%S\ start...`" >> $LOG_FILE
echo -e  "\n resouce xbstream file name:$FULL_RS_PATH/full_backup.xbstream" >> $LOG_FILE
$XBSTREAM -x < $FULL_RS_PATH/full_backup.xbstream -C $FULL_RESTORE_TEMP_PATH
if [ $? -nt 0 ]; then 
	echo "xbstream ---> file failed,please check..." >> $LOG_FILE | mail -s "$HOSTNAME\'s xbstream--->file job error" zhoubingzheng@letv.com 
fi
echo -e  "\n `date +%Y.%m.%d\ %R:%S\ end...`" >> $LOG_FILE

echo -e  "\n === use qpress to unpacking === \n `date +%Y.%m.%d\ %R:%S\ start...`" >> $LOG_FILE
cd $FULL_RESTORE_TEMP_PATH
for f in `find ./ -iname "*\.qp"`; do $QPRESS -dT2 $f  $(dirname $f) && rm -f $f; done
echo -e  "\n `date +%Y.%m.%d\ %R:%S\ end...`" >> $LOG_FILE

echo -e  "\n === prepare the mysql data file === \n `date +%Y.%m.%d\ %R:%S\ start...`" >> $LOG_FILE
$INNOBACKUPEX --apply-log --use-memory=10G $FULL_RESTORE_TEMP_PATH --defaults-file=$MY_CNF_PATH
if [ $? -nt 0 ]; then 
	echo "prepare mysql data file failed,please check..." >> $LOG_FILE | mail -s "$HOSTNAME\'s prepare mysql data file job error" zhoubingzheng@letv.com 
fi
echo -e  "\n `date +%Y.%m.%d\ %R:%S\ end...`" >> $LOG_FILE

echo -e  "\n === close mysqld === \n `date +%Y.%m.%d\ %R:%S\ start...`" >> $LOG_FILE
service mcluster-mysqld stop
echo -e  "\n `date +%Y.%m.%d\ %R:%S\ end...`" >> $LOG_FILE

echo -e  "\n === backup old mysql data file and mv new data to mysql data path and restore the backup === \n `date +%Y.%m.%d\ %R:%S\ start...`" >> $LOG_FILE
rm -rf ${MYSQL_DATA_PATH}/mysql.old
mv $MYSQL_DATA_FILE_PATH ${MYSQL_DATA_PATH}/mysql.old
mkdir -p $MYSQL_DATA_FILE_PATH
cd $MYSQL_DATA_FILE_PATH
$INNOBACKUPEX --copy-back -rsync $FULL_RESTORE_TEMP_PATH --defaults-file=$MY_CNF_PATH
if [ $? -nt 0 ]; then 
	echo "restore mysql data file failed,please check..." >> $LOG_FILE | mail -s "$HOSTNAME\'s restore mysql data file job error" zhoubingzheng@letv.com 
fi
chown -R mysql:mysql $MYSQL_DATA_FILE_PATH

if [ -d $i ]; then
	rm -rf $FULL_RESTORE_TEMP_PATH
fi
echo -e  "\n `date +%Y.%m.%d\ %R:%S\ end...`" >> $LOG_FILE

echo  -e "\n `date +%Y.%m.%d\ %R:%S\ Restore` processing Finished!!! Please notice: a)remove ib_datafile0 and ib_datafile1 of other machine, b)modify the sst user and password with same as target server, c)restart all machine of mcluster" >> $LOG_FILE

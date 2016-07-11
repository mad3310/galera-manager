#!/bin/bash
HOST=$1
PROXY=$2
URL=$3
API_MCLUSTER_FILE=/usr/local/zabbix/$4_$5.txt
CHECK_API_MCLUSTER_FILE=/usr/local/zabbix/tmp/$4_$5.log
cat /dev/null > $API_MCLUSTER_FILE
cat /dev/null > $CHECK_API_MCLUSTER_FILE
$URL > /usr/local/zabbix/tmp/$4_$5.tmp
sed "s#^#$HOST #g" /usr/local/zabbix/tmp/$4_$5.tmp > $API_MCLUSTER_FILE
if [[ -s $API_MCLUSTER_FILE ]]
   then
      /usr/local/zabbix/bin/zabbix_sender -z $PROXY -i $API_MCLUSTER_FILE 2>>$CHECK_API_MCLUSTER_FILE 1>>$CHECK_API_MCLUSTER_FILE
       Failed=`cat $CHECK_API_MCLUSTER_FILE|grep -Eic "Failed 0|failed: 0"`
      if [ $Failed -eq 1 ]
        then
         echo "OK"
        else
         echo "`cat $CHECK_API_MCLUSTER_FILE|grep -i  Failed`"
      fi
    else
        echo "ERROR"
fi

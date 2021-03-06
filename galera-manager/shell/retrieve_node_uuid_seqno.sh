#! /bin/bash

TARGET_TEXT='mysqld_safe WSREP: Recovered position'

UUID=`cat /srv/mcluster/mysql/grastate.dat | grep uuid: | cut -c10-`
SEQNO=`cat /srv/mcluster/mysql/grastate.dat | grep seqno: | cut -c10-`

LOG_UUID=`cat /var/log/mcluster-mysqld.log | grep "$TARGET_TEXT" | tail -n 1 | cut -c55-90`
LOG_SEQNO=`cat /var/log/mcluster-mysqld.log | grep "$TARGET_TEXT" | tail -n 1 | cut -c92-`

ERROR_TARGET_UUID='00000000-0000-0000-0000-000000000000'
ERROR_TARGET_SEQNO='-1'

if [[ $UUID == $ERROR_TARGET_UUID ]] || [[ $SEQNO = $ERROR_TARGET_SEQNO ]] ; 
then
    rm /var/lib/mysql/mysql.sock
    service mcluster-mysqld start --wsrep-recover
    echo 'uuid:'$LOG_UUID
    echo 'seqno:'$LOG_SEQNO
    sed -i "s|^uuid:.*$|uuid:    $LOG_UUID|" /srv/mcluster/mysql/grastate.dat
    sed -i "s|^seqno:.*$|seqno:   $LOG_SEQNO|" /srv/mcluster/mysql/grastate.dat
else
    echo 'uuid:'$UUID
    echo 'seqno:'$SEQNO
fi

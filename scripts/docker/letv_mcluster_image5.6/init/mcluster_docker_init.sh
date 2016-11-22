#! /bin/bash

function checkvar(){
  if [ ! $2 ]; then
    echo ERROR: need  $1
    exit 1
  fi
}

IFACE=${IFACE:-pbond0}

checkvar IP $IP
checkvar NETMASK $NETMASK
checkvar GATEWAY $GATEWAY

#hosts
umount /etc/hosts
cat > /etc/hosts <<EOF
127.0.0.1 localhost
$IP	`hostname`
EOF
echo 'set host successfully'

#network
cat > /etc/sysconfig/network-scripts/ifcfg-$IFACE << EOF
DEVICE=$IFACE
ONBOOT=yes
BOOTPROTO=static
IPADDR=$IP
NETMASK=$NETMASK
GATEWAY=$GATEWAY
EOF
ifconfig $IFACE $IP/16
echo 'set network successfully'

#route
gateway=`echo $IP | cut -d. -f1,2`.0.1
route add default gw $gateway
route del -net 0.0.0.0 netmask 0.0.0.0 dev eth0

#my.cnf
if [ ! -d "/srv/mcluster/tmp" ]; then
  mkdir /srv/mcluster/tmp
  chown -R mysql:mysql /srv/mcluster/tmp
fi
is_wr_mycnf=`grep wsrep_node_address /opt/letv/mcluster/root/etc/my.cnf|wc -l`
if [ $is_wr_mycnf -eq 0 ]
then
sed -e "/\[mysqld_safe\]/i\wsrep_node_address=${IP}" -i /opt/letv/mcluster/root/etc/my.cnf
sed -e "/\[mysqld_safe\]/i\expire-logs-days = 14" -i /opt/letv/mcluster/root/etc/my.cnf
sed -e "/\[mysqld_safe\]/i\innodb_file_per_table = 1" -i /opt/letv/mcluster/root/etc/my.cnf
sed -e "/\[mysqld_safe\]/i\skip-name-resolve" -i /opt/letv/mcluster/root/etc/my.cnf
sed -e "/\[mysqld_safe\]/i\max_connections=5000" -i /opt/letv/mcluster/root/etc/my.cnf
sed -e "/\[mysqld_safe\]/i\max_user_connections=200" -i /opt/letv/mcluster/root/etc/my.cnf
sed -e "/\[mysqld_safe\]/i\innodb_buffer_pool_size=2G" -i /opt/letv/mcluster/root/etc/my.cnf
sed -e "/\[mysqld_safe\]/i\innodb_log_file_size=1024M" -i /opt/letv/mcluster/root/etc/my.cnf
sed -e "/\[mysqld_safe\]/i\innodb_log_buffer_size=256M" -i /opt/letv/mcluster/root/etc/my.cnf
sed -e "/\[mysqld_safe\]/i\tmpdir=/srv/mcluster/tmp" -i /opt/letv/mcluster/root/etc/my.cnf
sed -e "/\[mysqld_safe\]/i\innodb_stats_on_metadata=0" -i /opt/letv/mcluster/root/etc/my.cnf
sed -e "/\[mysqld_safe\]/i\log_slave_updates=1" -i /opt/letv/mcluster/root/etc/my.cnf
sed -e "/\[mysqld_safe\]/i\wsrep_log_conflicts=1" -i /opt/letv/mcluster/root/etc/my.cnf
sed -e "/\[mysqld_safe\]/i\wsrep_provider_options='cert.log_conflicts=YES'" -i /opt/letv/mcluster/root/etc/my.cnf
sed -e "/\[mysqld_safe\]/i\slow_query_log=1" -i /opt/letv/mcluster/root/etc/my.cnf
sed -e "/\[mysqld_safe\]/i\long_query_time=1" -i /opt/letv/mcluster/root/etc/my.cnf
sed -e "/\[mysqld_safe\]/i\innodb_flush_log_at_trx_commit=2" -i /opt/letv/mcluster/root/etc/my.cnf
sed "s/#wsrep_node_name=node1/wsrep_node_name=node1/g" -i /opt/letv/mcluster/root/etc/my.cnf
echo 'set my.cnf successuflly'
fi

#init root
iscreate_root=`grep -c "GRANT ALL PRIVILEGES ON *.* TO 'root'@'127.0.0.1' IDENTIFIED BY 'Mcluster' WITH GRANT OPTION;" /usr/share/mysql/mcluster-bootstrap`
iscreate_backup=`grep -c "GRANT RELOAD, LOCK TABLES, REPLICATION CLIENT, CREATE TABLESPACE, SUPER ON *.* TO 'backup'@'%' IDENTIFIED BY 'backup';" /usr/share/mysql/mcluster-bootstrap`
ischange_monitor=`grep -c "GRANT ALL ON monitor.* to 'monitor'@'%' WITH MAX_QUERIES_PER_HOUR 36000 MAX_UPDATES_PER_HOUR 36000 MAX_CONNECTIONS_PER_HOUR 36000 MAX_USER_CONNECTIONS 36000;" /usr/share/mysql/mcluster-bootstrap`
if [ $iscreate_root -eq 0 ]
then
sed -i "/DROP DATABASE test/i\GRANT ALL PRIVILEGES ON *.* TO 'root'@'127.0.0.1' IDENTIFIED BY 'Mcluster' WITH GRANT OPTION;" /usr/share/mysql/mcluster-bootstrap
fi
if [ $iscreate_backup -eq 0 ]
then
sed -i "/DROP DATABASE test/i\GRANT RELOAD, LOCK TABLES, REPLICATION CLIENT, CREATE TABLESPACE, SUPER ON *.* TO 'backup'@'%' IDENTIFIED BY 'backup';" /usr/share/mysql/mcluster-bootstrap
fi
if [ $ischange_monitor -eq 0 ]
then
sed -i "/FLUSH PRIVILEGES/i\GRANT ALL ON monitor.* to 'monitor'@'%' WITH MAX_QUERIES_PER_HOUR 36000 MAX_UPDATES_PER_HOUR 36000 MAX_CONNECTIONS_PER_HOUR 36000 MAX_USER_CONNECTIONS 36000;" /usr/share/mysql/mcluster-bootstrap
fi


service crond restart

# init salt
sed -i "42s/.*/id: $(hostname)/" /etc/salt/minion
service salt-minion start
echo 'set init salt successuflly'


#set logrotate
cat > /etc/logrotate.d/mysql_log << EOF
/var/log/mcluster-mysqld.log 
{
daily
dateext
dateformat -%Y%m%d-%s
nocompress
size 50M
missingok
notifempty
copytruncate
rotate 5
postrotate
endscript
}
EOF

MYSQL_SLOW_LOG=`hostname`
cat > /etc/logrotate.d/mysql_slow_log << EOF
/srv/mcluster/mysql/${MYSQL_SLOW_LOG}-slow.log
{
daily
dateext
dateformat -%Y%m%d-%s
nocompress
size 50M
missingok
notifempty
copytruncate
rotate 5
postrotate
endscript
}
EOF

cat > /etc/logrotate.d/monit << EOF
/var/log/monit.log
{
daily
dateext
dateformat -%Y%m%d-%s
nocompress
size 1M
missingok
notifempty
copytruncate
rotate 5
postrotate
endscript
}
EOF

is_logrotate=`grep -c "logrotate" /etc/crontab`
if [ ${is_logrotate} -eq 0 ]
then
echo '0 * * * * root /usr/sbin/logrotate /etc/logrotate.conf >/dev/null 2>&1' >> /etc/crontab
fi

echo 'set logrotate successfully'


#install logstash



# set monit
cat >  /etc/monitrc  << EOF
set daemon 30
set logfile /var/log/monit.log
set pidfile /var/run/monit.pid
set httpd port 30000
allow 127.0.0.1

#data node
check process logstash MATCHING '/opt/logstash-forwarder/bin/logstash-forwarder -config /etc/logstash-forwarder.conf'
    start program = "/etc/init.d/logstash-forwarder start"
    stop  program = "/etc/init.d/logstash-forwarder stop"

check process salt_minion MATCHING '/usr/bin/python /usr/bin/salt-minion'
    start program = "/etc/init.d/salt-minion start"
    stop  program = "/etc/init.d/salt-minion stop"    
EOF

/etc/init.d/monit start

echo 'set monit successfully'

$@


#!/bin/bash

is_wr_mycnf=`grep wsrep_node_address /opt/letv/mcluster/root/etc/my.cnf|wc -l`
if [ $is_wr_mycnf -eq 0 ]
then
sed -e "/\[mysqld_safe\]/i\wsrep_node_address=" -i /opt/letv/mcluster/root/etc/my.cnf
sed -e "/\[mysqld_safe\]/i\expire-logs-days = 14" -i /opt/letv/mcluster/root/etc/my.cnf
sed -e "/\[mysqld_safe\]/i\innodb_file_per_table = 1" -i /opt/letv/mcluster/root/etc/my.cnf
sed -e "/\[mysqld_safe\]/i\skip-name-resolve" -i /opt/letv/mcluster/root/etc/my.cnf
sed -e "/\[mysqld_safe\]/i\max_connections=500" -i /opt/letv/mcluster/root/etc/my.cnf
sed -e "/\[mysqld_safe\]/i\max_user_connections=200" -i /opt/letv/mcluster/root/etc/my.cnf
sed -e "/\[mysqld_safe\]/i\innodb_buffer_pool_size=256M" -i /opt/letv/mcluster/root/etc/my.cnf
sed -e "/\[mysqld_safe\]/i\innodb_log_file_size=256M" -i /opt/letv/mcluster/root/etc/my.cnf
sed -e "/\[mysqld_safe\]/i\innodb_log_buffer_size=256M" -i /opt/letv/mcluster/root/etc/my.cnf
sed -e "/\[mysqld_safe\]/i\tmpdir=/srv/mcluster/tmp" -i /opt/letv/mcluster/root/etc/my.cnf
sed "s/#wsrep_node_name=node1/wsrep_node_name=node1/g" -i /opt/letv/mcluster/root/etc/my.cnf
echo 'set my.cnf successuflly'
fi

iscreate_root=`grep -c "GRANT ALL PRIVILEGES ON *.* TO 'root'@'127.0.0.1' IDENTIFIED BY 'Mcluster' WITH GRANT OPTION;" /usr/share/mysql/mcluster-bootstrap`
iscreate_backup=`grep -c "GRANT RELOAD, LOCK TABLES, REPLICATION CLIENT, CREATE TABLESPACE, SUPER ON *.* TO 'backup'@'%' IDENTIFIED BY 'backup';" /usr/share/mysql/mcluster-bootstrap`
if [ $iscreate_root -eq 0 ]
then
sed -i "/DROP DATABASE test/i\GRANT ALL PRIVILEGES ON *.* TO 'root'@'127.0.0.1' IDENTIFIED BY 'Mcluster' WITH GRANT OPTION;" /usr/share/mysql/mcluster-bootstrap
fi
if [ $iscreate_backup -eq 0 ]
then
sed -i "/DROP DATABASE test/i\GRANT RELOAD, LOCK TABLES, REPLICATION CLIENT, CREATE TABLESPACE, SUPER ON *.* TO 'backup'@'%' IDENTIFIED BY 'backup';" /usr/share/mysql/mcluster-bootstrap
fi

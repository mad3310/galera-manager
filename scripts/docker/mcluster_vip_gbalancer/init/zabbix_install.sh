#!/bin/bash
A=`pwd`
###########install zabbix################################
useradd -d /usr/local/zabbix -m  zabbix
echo 'zabbix:6flu@CQfz%'|chpasswd 
cd  /usr/local/zabbix
#if [ `curl -Is --connect-timeout 3 --max-time 3 "http://115.182.93.59:8080/zabbix/zabbix_agents.tar.gz"|grep -c 'HTTP/1.1 200 OK'` -eq 1 ]
#then
#	wget -O zabbix_agents.tar.gz http://115.182.93.59:8080/zabbix/zabbix_agents.tar.gz
#else
#	wget -O zabbix_agents.tar.gz http://10.200.93.59:8080/zabbix/zabbix_agents.tar.gz
#fi
#tar xfz zabbix_agents.tar.gz 
#sleep 5
chown -R zabbix.zabbix /usr/local/zabbix
sleep 5
/usr/local/zabbix/install.sh
sleep 5
sed -i '/zabbix_agentd/d' /etc/rc.d/rc.local
echo "/usr/local/zabbix/sbin/zabbix_agentd -c /usr/local/zabbix/conf/zabbix_agentd.conf start" >> /etc/rc.d/rc.local
#############add sudo####################################
HOSTNAME=`hostname`
Uname=`uname`
if [ "$Uname" = "linux" ] || [ "$Uname" = "Linux" ]
then
	if [ -f /etc/sudoers ]
	then
		sed -i "s/^Defaults.*requiretty/#Defaults    requiretty/" /etc/sudoers 
		sed -i '/zabbix/d' /etc/sudoers
		echo "zabbix          ALL=NOPASSWD: /sbin/ethtool" >> /etc/sudoers
		echo "zabbix          ALL=NOPASSWD: /bin/cat" >> /etc/sudoers
		echo "zabbix          ALL=NOPASSWD: /usr/sbin/mtr" >> /etc/sudoers
		echo "zabbix          ALL=NOPASSWD: /bin/ping" >> /etc/sudoers
		echo "zabbix          ALL=NOPASSWD: /usr/sbin/megacli64" >> /etc/sudoers
		echo "zabbix          ALL=NOPASSWD: /usr/sbin/hpacucli" >> /etc/sudoers
		echo "zabbix          ALL=NOPASSWD: /usr/bin/lsiutil" >> /etc/sudoers
		echo "zabbix          ALL=NOPASSWD: /usr/bin/ipmitool" >> /etc/sudoers
		echo "zabbix          ALL=NOPASSWD: /sbin/ipvsadm" >> /etc/sudoers
		echo "zabbix          ALL=NOPASSWD: /usr/sbin/hwconfig" >> /etc/sudoers
		echo "zabbix          ALL=NOPASSWD: /usr/sbin/dmidecode" >> /etc/sudoers
		echo "zabbix          ALL=NOPASSWD: /usr/sbin/ss" >> /etc/sudoers
		echo "zabbix          ALL=NOPASSWD: /usr/bin/tail" >> /etc/sudoers
		echo "zabbix          ALL=NOPASSWD: /usr/bin/iotop" >> /etc/sudoers
		if [ -z "`cat /etc/hosts | grep $HOSTNAME | grep 127.0.0.1`" ]
		then
			sed -i "s/^127.0.0.1.*/& $HOSTNAME/" /etc/hosts
		fi
	else
		echo "sudoers file not found"
	fi
	if [ -s /usr/sbin/ethtool -a ! -s /sbin/ethtool ]
	then
		ln -s /usr/sbin/ethtool /sbin/ethtool
	fi
fi


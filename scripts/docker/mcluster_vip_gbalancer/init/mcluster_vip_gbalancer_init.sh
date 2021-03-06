#! /bin/bash
function checkvar(){
  if [ ! $2 ]; then
    echo ERROR: need  $1
    exit 1
  fi
}

IFACE=${IFACE:-peth1}

checkvar IP $IP
checkvar NETMASK $NETMASK
checkvar GATEWAY $GATEWAY

#hosts
umount /etc/hosts
cat > /etc/hosts << EOF
127.0.0.1 localhost
$IP     `hostname`
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
ifconfig $IFACE $IP/24
echo 'set network successfully'

#route
gateway=`echo $IP | cut -d. -f1,2`.91.1
route del -net 0.0.0.0 netmask 0.0.0.0 dev eth0
route add default gw $gateway

# init salt
sed -i "42s/.*/id: $(hostname)/" /etc/salt/minion
service salt-minion start
echo 'set init salt successfully'


cat >  /etc/monitrc  << EOF
set daemon 30
set logfile /var/log/monit.log
set pidfile /var/run/monit.pid
set httpd port 30000
allow 127.0.0.1

#vip node
check process 8888gbalancer MATCHING '/usr/bin/gbalancer --config=/etc/gbalancer/8888configuration.json -daemon'
    start program = "/usr/local/init/check_gbalancer.sh 8888"
    stop  program = "/usr/local/init/check_gbalancer.sh 8888"

check process 3306gbalancer MATCHING '/usr/bin/gbalancer --config=/etc/gbalancer/3306configuration.json -daemon'
    start program = "/usr/local/init/check_gbalancer.sh 3306"
    stop  program = "/usr/local/init/check_gbalancer.sh 3306"

check process salt_minion MATCHING '/usr/bin/python /usr/bin/salt-minion'
    start program = "/etc/init.d/salt-minion start"
    stop  program = "/etc/init.d/salt-minion stop"    
        
EOF
/etc/init.d/monit start
echo 'set monit successfully'

service crond restart

#zabbix init
if [ ! -d "/usr/local/zabbix/conf" ]; then
cd /usr/local/zabbix/
tar -xvzf zabbix_agents.tar.gz
fi
cd /usr/local
chmod 775 zabbix_install.sh
./zabbix_install.sh
cd /usr/local/zabbix/conf
touch host.temp
echo "${IP},115.182.93.64,218.206.201.236,211.162.59.93,114.80.187.245,114.80.187.246,121.14.196.239,10.0.51.13" > host.temp
service crond restart
cd /usr/local/zabbix
./install.sh
./check_zabbix.sh

$@

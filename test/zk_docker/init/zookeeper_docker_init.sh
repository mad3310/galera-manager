#! /bin/bash

function checkvar(){
  if [ ! $2 ]; then
    echo ERROR: need  $1
    exit 1
  fi
}

#check env
checkvar ZKID $


#zoo.cfg
mkdir -p /etc/zookeeper/conf/
cat > /etc/zookeeper/conf/zoo.cfg << EOF
tickTime=2000
initLimit=10
syncLimit=5
dataDir=/var/lib/zookeeper
clientPort=2181
maxClientCnxns=100
server.1=
server.2=
server.3=
EOF

echo $ZKID > /etc/zookeeper/conf/myid
mkdir /var/lib/zookeeper
ln -s /etc/zookeeper/conf/myid /var/lib/zookeeper/myid
echo 'set zoo.cf and myid successuflly'

#crond init
echo "1 */6 * * * root java -cp /usr/local/zookeeper/zookeeper-3.4.6.jar:/usr/local/zookeeper/lib/log4j-1.2.16.jar:/usr/local/zookeeper/lib/slf4j-api-1.6.1.jar:/usr/local/zookeeper/lib/slf4j-log4j12-1.6.1.jar:conf org.apache.zookeeper.server.PurgeTxnLog /var/lib/zookeeper/ -n 3" >> /etc/crontab

$@

#! /bin/sh

mclustermanager3="172.17.0.85"
mclustermanager2="172.17.0.84"
mclustermanager1="172.17.0.83"

zookeeper3="172.17.0.34"
zookeeper2="172.17.0.33"
zookeeper1="172.17.0.32"

curl "http://${mclustermanager1}:8888/admin/reset"

curl "http://${mclustermanager2}:8888/admin/reset"

curl "http://${mclustermanager3}:8888/admin/reset"

curl -d "adminUser=root&adminPassword=root" "http://${mclustermanager1}:8888/admin/user"

curl -d "adminUser=root&adminPassword=root" "http://${mclustermanager2}:8888/admin/user"

curl -d "adminUser=root&adminPassword=root" "http://${mclustermanager3}:8888/admin/user"

curl -d "zkAddress=${zookeeper1}&zkPort=2181" "http://${mclustermanager1}:8888/admin/conf"

curl -d "zkAddress=${zookeeper2}&zkPort=2181" "http://${mclustermanager2}:8888/admin/conf"

curl -d "zkAddress=${zookeeper3}&zkPort=2181" "http://${mclustermanager3}:8888/admin/conf"

curl --user root:root -d "clusterName=letv_mcluster_test_1&dataNodeIp=${mclustermanager1}&dataNodeName=letv_mcluster_test_1_node_1" "http://${mclustermanager1}:8888/cluster"

curl --user root:root "http://${mclustermanager1}:8888/cluster/init?forceInit=false"

curl "http://${mclustermanager2}:8888/cluster/sync"

curl --user root:root -d "dataNodeIp=${mclustermanager2}&dataNodeName=letv_mcluster_test_1_node_2" "http://${mclustermanager2}:8888/cluster/node"

curl "http://${mclustermanager3}:8888/cluster/sync"

curl --user root:root -d "dataNodeIp=${mclustermanager3}&dataNodeName=letv_mcluster_test_1_node_3" "http://${mclustermanager3}:8888/cluster/node"

curl --user root:root -d "cluster_flag=new" "http://${mclustermanager1}:8888/cluster/start"

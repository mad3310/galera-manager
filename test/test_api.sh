#! /bin/sh

export docker_ip=""
function get_docker_ip(){
	local docker_name=$1
	
	docker_ip=`docker inspect ${docker_name} | grep "IPAddress" | awk '{printf $(2)}' | sed -n -e 's/"//gp' | sed -n -e 's/,//gp'`
}

get_docker_ip 'mcluster-manager-test-3'
mclustermanager3=${docker_ip}

get_docker_ip 'mcluster-manager-test-2'
mclustermanager2=${docker_ip}

get_docker_ip 'mcluster-manager-test-1'
mclustermanager1=${docker_ip}

get_docker_ip 'manager-zk-3'
zookeeper3=${docker_ip}

get_docker_ip 'manager-zk-2'
zookeeper2=${docker_ip}

get_docker_ip 'manager-zk-1'
zookeeper1=${docker_ip}

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

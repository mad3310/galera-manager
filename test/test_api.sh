#! /bin/sh

export docker_ip=""
function get_docker_ip(){
	local docker_name=$1
	
	docker_ip=`docker inspect ${docker_name} | grep "IPAddress" | awk '{printf $(2)}' | sed -n -e 's/"//gp' | sed -n -e 's/,//gp'`
}

function test_stat_interface(){
	local ip1=$1
	local ip2=$2
	local ip3=$3
	
	for ip in $ip1 $ip2 $ip3
	do
		curl "http://${ip}:8888/all/stat"

		curl "http://${ip}:8888/db/all/stat/rowsoper/total"        
		
		curl "http://${ip}:8888/db/all/stat/rowsoper/ps"        
		
		curl "http://${ip}:8888/db/all/stat/innobuffer/memallco"        
		
		curl "http://${ip}:8888/db/all/stat/innobuffer/page"        
		
		curl "http://${ip}:8888/db/all/stat/innobuffer/pool"        
		
		curl "http://${ip}:8888/db/all/stat/variablestatus/ps"        
		
		curl "http://${ip}:8888/db/all/stat/variablestatus/used"        
		
		curl "http://${ip}:8888/db/all/stat/variablestatus/ration"      
		
		curl "http://${ip}:8888/db/all/stat/wsrepstatus/flow_control_paused"        
		
		curl "http://${ip}:8888/db/all/stat/wsrepstatus/slowest_node_param"        
		
		curl "http://${ip}:8888/db/all/stat/wsrepstatus/slowest_network_param"        
		
		curl "http://${ip}:8888/inner/node/check/log/error"
		
		curl "http://${ip}:8888/inner/node/check/log/warning"
		
		curl "http://${ip}:8888/inner/node/check/log/health"
		
		curl "http://${ip}:8888/node/stat"
		
		curl "http://${ip}:8888/node/stat/datadir/size"     
	done
}

function test_create_cluster(){
	local ip1=$1
	local ip2=$2
	local ip3=$3
	
	curl --user root:root -d "clusterName=mcluster-manager-test&dataNodeIp=${ip1}&dataNodeName=d-mcl-mcluster-manager-test-n-1" "http://${ip1}:8888/cluster"

	curl --user root:root "http://${ip1}:8888/cluster/init?forceInit=false"
	
	curl --user root:root -d "dbName=mcluster-manager&userName=mcluster-manager" "http://localhost:8888/db" 
	
	curl "http://${ip2}:8888/cluster/sync"
	
	curl --user root:root -d "dataNodeIp=${ip2}&dataNodeName=d-mcl-mcluster-manager-test-n-2" "http://${ip2}:8888/cluster/node"
	
	curl "http://${ip3}:8888/cluster/sync"
	
	curl --user root:root -d "dataNodeIp=${ip3}&dataNodeName=d-mcl-mcluster-manager-test-n-3" "http://${ip3}:8888/cluster/node"
	
	curl --user root:root -d "cluster_flag=new" "http://${ip1}:8888/cluster/start"
	
	sleep 120
}


function test_reset_cluster(){
	local ip1=$1
	local ip2=$2
	local ip3=$3
	
	for ip in $ip1 $ip2 $ip3
	do
		curl "http://${ip}:8888/admin/reset"
	done
}


function test_set_admin_for_cluster(){
	local ip1=$1
	local ip2=$2
	local ip3=$3
	
	for ip in $ip1 $ip2 $ip3
	do
		curl -d "adminUser=root&adminPassword=root" "http://${ip}:8888/admin/user"
	done
	
	get_docker_ip 'manager-zk-3'
	zookeeper3=${docker_ip}
	
	get_docker_ip 'manager-zk-2'
	zookeeper2=${docker_ip}
	
	get_docker_ip 'manager-zk-1'
	zookeeper1=${docker_ip}
	
	curl -d "zkAddress=${zookeeper1}&zkPort=2181" "http://${ip1}:8888/admin/conf"

	curl -d "zkAddress=${zookeeper2}&zkPort=2181" "http://${ip2}:8888/admin/conf"

	curl -d "zkAddress=${zookeeper3}&zkPort=2181" "http://${ip3}:8888/admin/conf"
}

get_docker_ip 'd-mcl-mcluster-manager-test-n-3'
mclustermanager3=${docker_ip}

get_docker_ip 'd-mcl-mcluster-manager-test-n-2'
mclustermanager2=${docker_ip}

get_docker_ip 'd-mcl-mcluster-manager-test-n-1'
mclustermanager1=${docker_ip}

echo $mclustermanager1 $mclustermanager2 $mclustermanager3

test_reset_cluster $mclustermanager1 $mclustermanager2 $mclustermanager3
test_set_admin_for_cluster $mclustermanager1 $mclustermanager2 $mclustermanager3
test_create_cluster $mclustermanager1 $mclustermanager2 $mclustermanager3
test_stat_interface $mclustermanager1 $mclustermanager2 $mclustermanager3


#!/bin/bash

IMAGE="letv/zookeeper-test:0.0.2"

function create_zk_docker_container(){
	local docker_name=$1
	local zkid=$2
	
    docker run -i -t --rm --privileged --memory="128m" -h $docker_name \
	--env "ZKID=$zkid" \
	--name $docker_name $IMAGE
	
}

export docker_ip=""
function get_docker_ip(){
	local docker_name=$1
	
	docker_ip=`docker inspect ${docker_name} | grep "IPAddress" | awk '{printf $(2)}' | sed -n -e 's/"//gp' | sed -n -e 's/,//gp'`
}

function remove_docker_container(){
	local docker_name=$1
	
	docker stop $1
	docker rm $1
}

export docker_id=""
function get_docker_id(){
	local docker_name=$1
	
	docker_id=`docker inspect ${docker_name} | grep Id | awk '{printf $(2)}' | sed -n -e 's/"//gp' | sed -n -e 's/,//gp'`
}

function replace_zk_ip(){
#	sed "s/#server.1=zookeeper1:2888:3888/server.1=$1:2888:3888/g" -i /srv/docker/devicemapper/mnt/$4/rootfs/etc/zookeeper/conf/zoo.cfg
#	sed "s/#server.2=zookeeper2:2888:3888/server.2=$2:2888:3888/g" -i /srv/docker/devicemapper/mnt/$4/rootfs/etc/zookeeper/conf/zoo.cfg
#	sed "s/#server.3=zookeeper3:2888:3888/server.3=$3:2888:3888/g" -i /srv/docker/devicemapper/mnt/$4/rootfs/etc/zookeeper/conf/zoo.cfg
	echo "server.1=$1:2888:3888" >> /srv/docker/devicemapper/mnt/$4/rootfs/etc/zookeeper/conf/zoo.cfg
	echo "server.2=$2:2888:3888" >> /srv/docker/devicemapper/mnt/$4/rootfs/etc/zookeeper/conf/zoo.cfg
	echo "server.3=$3:2888:3888" >> /srv/docker/devicemapper/mnt/$4/rootfs/etc/zookeeper/conf/zoo.cfg
}

remove_docker_container 'manager-zk-1'
remove_docker_container 'manager-zk-2'
remove_docker_container 'manager-zk-3'

create_zk_docker_container 'manager-zk-1' 1
create_zk_docker_container 'manager-zk-2' 2
create_zk_docker_container 'manager-zk-3' 3

get_docker_ip 'manager-zk-1'
docker_ip_1=${docker_ip}
echo ${docker_ip_1}

get_docker_ip 'manager-zk-2'
docker_ip_2=${docker_ip}
echo ${docker_ip_2}

get_docker_ip 'manager-zk-3'
docker_ip_3=${docker_ip}
echo ${docker_ip_3}

get_docker_id 'manager-zk-1'
docker_id_1=${docker_id}
echo ${docker_id_1}

get_docker_id 'manager-zk-2'
docker_id_2=${docker_id}
echo ${docker_id_2}

get_docker_id 'manager-zk-3'
docker_id_3=${docker_id}
echo ${docker_id_3}

replace_zk_ip ${docker_ip_1} ${docker_ip_2} ${docker_ip_3} ${docker_id_1}
replace_zk_ip ${docker_ip_1} ${docker_ip_2} ${docker_ip_3} ${docker_id_2}
replace_zk_ip ${docker_ip_1} ${docker_ip_2} ${docker_ip_3} ${docker_id_3}




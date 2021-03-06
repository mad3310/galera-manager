#! /bin/bash

export docker_id=""
function get_docker_id(){
	local docker_name=$1
	
	docker_id=`docker inspect ${docker_name} | grep Id | awk '{printf $(2)}' | sed -n -e 's/"//gp' | sed -n -e 's/,//gp'`
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

function create_docker_container(){
	local docker_name=$1
	
	docker run -i -t --rm --privileged --memory="512m" -h $1 -v /srv/mcluster/  --name $1 letv/mcluster-manager-test:0.0.8 /bin/bash
}

function copy_src_to_container(){
	local docker_id=$1
	
	cp -r /opt/letv/mcluster-manager/api /srv/docker/devicemapper/mnt/$1/rootfs/opt/letv/mcluster-manager/
}

function write_container_ip_to_local(){
	local docker_id=$1
	local docker_ip=$2
    cat > /srv/docker/devicemapper/mnt/${docker_id}/rootfs/tmp/zkip <<EOF
zkip=${docker_ip}
EOF
}


remove_docker_container 'd-mcl-mcluster-manager-test-n-1'
remove_docker_container 'd-mcl-mcluster-manager-test-n-2'
remove_docker_container 'd-mcl-mcluster-manager-test-n-3'

create_docker_container 'd-mcl-mcluster-manager-test-n-1'
create_docker_container 'd-mcl-mcluster-manager-test-n-2'
create_docker_container 'd-mcl-mcluster-manager-test-n-3'

get_docker_id 'd-mcl-mcluster-manager-test-n-3'
docker_id_3=${docker_id}
echo ${docker_id_3}

get_docker_id 'd-mcl-mcluster-manager-test-n-2'
docker_id_2=${docker_id}
echo ${docker_id_2}

get_docker_id 'd-mcl-mcluster-manager-test-n-1'
docker_id_1=${docker_id}
echo ${docker_id_1}

copy_src_to_container ${docker_id_3}
copy_src_to_container ${docker_id_2}
copy_src_to_container ${docker_id_1}

get_docker_ip 'd-mcl-mcluster-manager-test-n-3'
docker_ip_3=${docker_ip}
echo ${docker_ip_3}

get_docker_ip 'd-mcl-mcluster-manager-test-n-2'
docker_ip_2=${docker_ip}
echo ${docker_ip_2}

get_docker_ip 'd-mcl-mcluster-manager-test-n-1'
docker_ip_1=${docker_ip}
echo ${docker_ip_1}

write_container_ip_to_local ${docker_id_3} ${docker_ip_3}
write_container_ip_to_local ${docker_id_2} ${docker_ip_2}
write_container_ip_to_local ${docker_id_1} ${docker_ip_1}

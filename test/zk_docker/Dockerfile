FROM 10.160.140.32:5000/letv-centos6
MAINTAINER bingzheng.zhou <zhoubingzheng@letv.com>

RUN rpm -ivh http://pkg-repo.oss.letv.com/pkgs/centos6/letv-release.noarch.rpm
RUN yum install cronie -y
RUN yum install vim -y
RUN yum update bash -y
RUN yum install java-1.7.0-openjdk-devel -y

EXPOSE 4567 4568 4569 2181 2888 3888
USER root

RUN mkdir -p /usr/local/zookeeper/

ADD ./init/zookeeper.tar.gz /usr/local/
ADD ./init/zookeeper_docker_init.sh /usr/local/zookeeper/zookeeper_docker_init.sh

RUN chmod 775 /usr/local/zookeeper/zookeeper_docker_init.sh

ENTRYPOINT /usr/local/zookeeper/zookeeper_docker_init.sh && /usr/local/zookeeper/bin/zkServer.sh start && /bin/bash
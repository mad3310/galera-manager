FROM letv:centos6
MAINTAINER bingzheng.zhou <zhoubingzheng@letv.com>

RUN yum --enablerepo=staging install mcluster-mysql-server -y
RUN yum install vim -y
RUN yum install python-tornado -y
RUN yum install kazoo -y
RUN yum install zope.interface -y
RUN yum install MySQL-python -y

RUN mkdir -p /srv/init/
RUN mkdir -p /opt/letv/mcluster-manager
RUN mkdir -p /var/log/mcluster-manager

WORKDIR /root/git/mcluster-manager

ADD ./init/test_docker_init.sh /srv/init/test_docker_init.sh

ENTRYPOINT /srv/init/test_docker_init.sh && /bin/bash
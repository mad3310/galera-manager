FROM 10.160.140.32:5000/letv-centos6
MAINTAINER bingzheng.zhou <zhoubingzheng@letv.com>

RUN yum clean all
RUN rpm -ivh http://pkg-repo.oss.letv.com/pkgs/centos6/letv-release.noarch.rpm 
RUN yum --enablerepo=staging install mcluster-mysql-server -y
RUN yum install innotop -y
RUN yum install percona-toolkit -y

RUN yum install vim -y
RUN yum install python-tornado -y
RUN yum install kazoo -y
RUN yum install zope.interface -y
RUN yum install MySQL-python -y
RUN yum install python-futures -y
RUN yum install mcluster-manager -y

EXPOSE 3306 8888 2888 3888
USER root
RUN mkdir -p /usr/local/mcluster/init/
RUN mkdir -p /opt/letv/mcluster-manager
RUN mkdir -p /var/log/mcluster-manager


ADD ./init/test_docker_init.sh /usr/local/mcluster/init/test_docker_init.sh

WORKDIR /opt/letv/mcluster-manager

RUN chmod 775 /usr/local/mcluster/init/test_docker_init.sh
RUN yum remove mcluster-manager -y

ENTRYPOINT /usr/local/mcluster/init/test_docker_init.sh && /bin/bash
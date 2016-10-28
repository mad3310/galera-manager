FROM letv:centos6
MAINTAINER bingzheng.zhou <zhoubingzheng@letv.com>

RUN rpm -ivh http://pkg-repo.oss.letv.com/pkgs/centos6/letv-release.noarch.rpm

# set yum repo
ADD ./init/test.repo /etc/yum.repos.d/test.repo
RUN chmod 755 /etc/yum.repos.d/test.repo
RUN yum clean all

RUN yum --enablerepo=staging install mcluster-mysql-server -y
RUN yum install innotop -y
RUN yum install percona-toolkit -y
RUN yum install cronie -y
RUN yum install vim -y
RUN yum install java-1.7.0-openjdk-devel -y
RUN yum update bash -y

EXPOSE 3306 4567 4568 4569 2181 2888 3888
USER root

RUN mkdir -p /usr/local/init/

ADD ./init/mcluster_docker_init.sh /usr/local/init/mcluster_docker_init.sh

ADD ./init/salt-minion-2014.7.0-3.el6.noarch.rpm /tmp/salt-minion-2014.7.0-3.el6.noarch.rpm
ADD ./init/salt-2014.7.0-3.el6.noarch.rpm /tmp/salt-2014.7.0-3.el6.noarch.rpm
RUN yum -y localinstall /tmp/salt-2014.7.0-3.el6.noarch.rpm /tmp/salt-minion-2014.7.0-3.el6.noarch.rpm
RUN rm -rf /etc/salt/*
ADD ./init/minion /etc/salt/minion

RUN chmod 775 /usr/local/init/mcluster_docker_init.sh

# install logstash
ADD ./init/logstash-forwarder-0.4.0-1.x86_64.rpm /tmp/logstash-forwarder-0.4.0-1.x86_64.rpm
RUN rpm -ivh /tmp/logstash-forwarder-0.4.0-1.x86_64.rpm
RUN rm -rf /etc/logstash-forwarder.conf

# install monit
RUN yum install -y wget
RUN yum install -y monit
ADD ./init/monit /etc/init.d/monit
RUN chmod 755 /etc/init.d/monit

RUN yum install mcluster-manager-version -y

ENTRYPOINT /usr/local/init/mcluster_docker_init.sh && service mcluster-manager start && /bin/bash
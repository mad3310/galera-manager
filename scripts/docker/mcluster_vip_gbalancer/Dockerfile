FROM letv:centos6
MAINTAINER bingzheng.zhou <zhoubingzheng@letv.com>
RUN rpm -ivh http://pkg-repo.oss.letv.com/pkgs/centos6/letv-release.noarch.rpm

# set yum repo
ADD ./init/test.repo /etc/yum.repos.d/test.repo
RUN chmod 755 /etc/yum.repos.d/test.repo
RUN yum clean all

RUN yum install vim -y
RUN yum install tar -y
RUN yum install cronie -y
RUN yum install gbalancer-manager -y
RUN yum install gbalancer -y
RUN yum update bash -y

EXPOSE 3306 8888 9888

USER root

RUN mkdir -p /usr/local/init/
ADD ./init/mcluster_vip_gbalancer_init.sh /usr/local/init/mcluster_vip_gbalancer_init.sh

RUN chmod 775 /usr/local/init/mcluster_vip_gbalancer_init.sh

#install salt-minion
ADD ./init/salt-minion-2014.7.0-3.el6.noarch.rpm /tmp/salt-minion-2014.7.0-3.el6.noarch.rpm
ADD ./init/salt-2014.7.0-3.el6.noarch.rpm /tmp/salt-2014.7.0-3.el6.noarch.rpm
RUN yum -y localinstall /tmp/salt-2014.7.0-3.el6.noarch.rpm /tmp/salt-minion-2014.7.0-3.el6.noarch.rpm
RUN rm -rf /etc/salt/*
ADD ./init/minion /etc/salt/minion

#gbalancer auto start
ADD ./init/check_gbalancer.sh /usr/local/init/check_gbalancer.sh
RUN chmod 755 /usr/local/init/check_gbalancer.sh

# install monit
RUN yum install -y wget
RUN yum install -y monit
ADD ./init/monit /etc/init.d/monit
RUN chmod 755 /etc/init.d/monit

#install zabbix
ADD ./init/zabbix_install.sh /usr/local/zabbix_install.sh
ADD ./init/zabbix_agents.tar.gz /usr/local/zabbix/
ADD ./init/check_api.mcluster.sh /usr/local/zabbix/script/check_api.mcluster.sh
RUN chmod 775 /usr/local/zabbix_install.sh
RUN chmod 755 /opt/letv/mcluster-manager/client/zabbix_mcluster_monitor_main.py
RUN chmod 755 /usr/local/zabbix/script/check_api.mcluster.sh

RUN yum install -y mcluster-manager-version

ENTRYPOINT /usr/local/init/mcluster_vip_gbalancer_init.sh  && service gbalancer-manager start && /bin/bash
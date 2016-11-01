FROM letv:centos6
MAINTAINER bingzheng.zhou <zhoubingzheng@letv.com>

#RUN rpm -ivh http://pkg-repo.oss.letv.com/pkgs/centos6/letv-release.noarch.rpm

# set yum repo
RUN rm -rf /etc/yum.repos.d/CentOS-Debuginfo.repo
RUN rm -rf /etc/yum.repos.d/CentOS-Media.repo
RUN rm -rf /etc/yum.repos.d/CentOS-Vault.repo
ADD ./init/letv-pkgs.repo /etc/yum.repos.d/letv-pkgs.repo
ADD ./init/add.repo /etc/yum.repos.d/add.repo
ADD ./init/test.repo /etc/yum.repos.d/test.repo
RUN chmod 755 /etc/yum.repos.d/letv-pkgs.repo
RUN chmod 755 /etc/yum.repos.d/add.repo
RUN chmod 755 /etc/yum.repos.d/test.repo
RUN yum clean all

#install wget
RUN yum install -y wget
RUN yum install -y tar

#add mysql user
RUN groupadd mysql
RUN useradd mysql -s /sbin/nologin -M -g mysql

#install percona xtrabackup&toolkit
RUN cd /tmp
RUN /usr/bin/wget --no-check-certificate https://10.154.29.92:443/v1/AUTH_gaoqiang3/oss/PXC56/percona-xtrabackup-24-2.4.3-1.el6.x86_64.rpm
RUN yum localinstall percona-xtrabackup-24-2.4.3-1.el6.x86_64.rpm -y
RUN /usr/bin/wget --no-check-certificate https://10.154.29.92:443/v1/AUTH_gaoqiang3/oss/PXC56/percona-toolkit-2.2.19-1.noarch.rpm
RUN yum localinstall percona-toolkit-2.2.19-1.noarch.rpm -y
RUN yum install -y perl-DBD-MySQL perl-DBI perl-IO-Socket-SSL.noarch socat nc libev  perl-DBD-MySQL perl-DBI numactl

#make mysql dir
RUN mkdir -p /srv/mcluster/mysql
RUN mkdir -p /srv/mcluster/tmp
RUN mkdir -p /srv/mcluster/binlog/
RUN mkdir -p /var/lib/mysql/
RUN mkdir -p /opt/letv/mcluster/root
RUN mkdir -p /opt/letv/mcluster/root/etc
RUN chown -R mysql:mysql /srv/mcluster
RUN chown -R mysql:mysql /var/lib/mysql
RUN chown mysql:mysql /opt/letv/mcluster/root
RUN chmod 755 /opt/letv/mcluster/root

#install pxc
RUN /usr/bin/wget --no-check-certificate https://10.154.29.92:443/v1/AUTH_gaoqiang3/oss/PXC56/Percona-XtraDB-Cluster-5.6.30-rel76.3-25.16.1.Linux.x86_64.ssl101.tar.gz
RUN tar -xvzf Percona-XtraDB-Cluster-5.6.30-rel76.3-25.16.1.Linux.x86_64.ssl101.tar.gz
RUN mv Percona-XtraDB-Cluster-5.6.30-rel76.3-25.16.1.Linux.x86_64.ssl101/* /opt/letv/mcluster/root/
RUN cp /opt/letv/mcluster/root/bin/mysqldump /usr/bin/mysqldump 
RUN ln -s /opt/letv/mcluster/root/bin/mysql /usr/local/bin/mysql
RUN cp /opt/letv/mcluster/root/lib/libgalera_smm.so /usr/lib64/libgalera_smm.so
RUN cp /opt/letv/mcluster/root/support-files/mysql.server /etc/init.d/mcluster-mysqld
RUN sed -i "s/\/mnt\/workspace\/percona-xtradb-cluster-5.6-binary-new\/label_exp\/centos6-64\/Percona-XtraDB-Cluster-5.6.30-76.3\/58\/usr\/local\/Percona-XtraDB-Cluster-5.6.30-rel76.3-25.16.1.Linux.x86_64.ssl101/\/opt\/letv\/mcluster\/root/g" /etc/init.d/mcluster-mysqld

#add default my.cnf and mcluster-bootstrap
ADD ./init/my.cnf /opt/letv/mcluster/root/etc/my.cnf
RUN rm -rf /etc/my.cnf
RUN ln -s /opt/letv/mcluster/root/etc/my.cnf /etc/my.cnf
ADD ./init/mcluster-bootstrap /usr/share/mysql/mcluster-bootstrap
RUN chmod 755 /usr/share/mysql/mcluster-bootstrap

#yum need software
RUN yum install cronie -y
RUN yum install vim -y
RUN yum update bash -y

#set innotop
ADD ./init/innotop /usr/local/bin/innotop
RUN chmod 755 /usr/local/bin/innotop

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

# install monit
RUN yum install -y wget
RUN yum install -y monit
ADD ./init/monit /etc/init.d/monit
RUN chmod 755 /etc/init.d/monit

#set mysql env
ENV PATH /opt/letv/mcluster/root/bin:$PATH

#RUN yum install mcluster-manager-version -y
RUN yum install mcluster-manager -y

ENTRYPOINT /usr/local/init/mcluster_docker_init.sh && service mcluster-manager start && /bin/bash

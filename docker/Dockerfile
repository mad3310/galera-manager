FROM letv:centos6
MAINTAINER bingzheng.zhou <zhoubingzheng@letv.com>

RUN yum install vim -y
RUN mkdir -p /srv/test/init/

ADD ./init/test_docker_init.sh /srv/test/init/test_docker_init.sh
ADD ./test                /root/test

ENTRYPOINT /srv/test/init/test_docker_init.sh && /bin/bash

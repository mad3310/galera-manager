#!/bin/bash

rm -rf /opt/letv/mcluster-manager
rm -rf /etc/init.d/mcluster-manager
rm -rf /etc/sysconfig/mcluster-manager
yum remove -y es_pack

exit 0

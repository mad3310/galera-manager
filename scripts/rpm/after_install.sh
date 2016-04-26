#!/bin/bash

cp /opt/letv/mcluster-manager/scripts/init.d/mcluster-manager_service /etc/init.d/mcluster-manager
cp /opt/letv/mcluster-manager/scripts/init.d/mcluster-manager_conf /etc/sysconfig/mcluster-manager
chmod +x /etc/init.d/mcluster-manager
chkconfig --add mcluster-manager
/etc/init.d/mcluster-manager start | stop | restart

exit 0

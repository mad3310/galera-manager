#!/bin/bash

chmod +x /etc/init.d/mcluster-manager
chkconfig --add mcluster-manager
/etc/init.d/mcluster-manager start | stop | restart

exit 0

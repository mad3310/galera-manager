#!/bin/bash

# 删除多余的simplejson依赖
rm -rf /usr/lib64/python2.6/site-packages/simplejson
rm -rf /usr/lib64/python2.6/site-packages/simplejson-2.0.9-py2.6.egg-info

chmod +x /etc/init.d/mcluster-manager
chkconfig --add mcluster-manager
/etc/init.d/mcluster-manager start | stop | restart

exit 0

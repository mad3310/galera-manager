# -*- coding: utf-8 -*-
from __future__ import absolute_import

from utils.storify import storify


MONITOR_TYPE = {'db': ['existed_db_anti_item',
                       'wsrep_status',
                       'cur_user_conns',
                       'cur_conns',
                       'write_read_avaliable'
                       ],

                'node': ['log_health',
                         'log_error',
                         'started'
                         ]
                }

# 数据库节点监控类型
DB_MONITOR_TYPE = storify(dict(
    EXISTED_DB_ANTI_ITEM='existed_db_anti_item',
    WSREP_STATUS='wsrep_status',
    CUR_USER_CONNS='cur_user_conns',
    CUR_CONNS='cur_conns',
    WRITE_REAL_AVALLIABLE='write_read_avaliable'
))

# 容器节点监控类型
NODE_MONITOR_TYPE = storify(dict(
    LOG_HEALTH='health',
    LOG_ERROR='log_error',
    STARTED='started'
))


# 监控警告等级
ALARM_LEVEL = storify(dict(
    SERIOUS='tel:sms:email',
    GENERAL='sms:email',
    NOTHING='nothing'
))

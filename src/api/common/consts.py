# -*- coding: utf-8 -*-

from src.api.utils.storify import storify

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

ALARM_LEVEL = storify(dict(
    SERIOUS='tel:sms:email',
    GENERAL='sms:email',
    NOTHING='nothing'
))

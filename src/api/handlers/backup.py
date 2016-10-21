# -*- coding: utf-8 -*-

import logging
import datetime

from tornado.web import asynchronous
from tornado.gen import engine

from base import APIHandler
from common.utils.exceptions import HTTPAPIErrorException
from common.tornado_basic_auth import require_basic_auth
from common.utils.asyc_utils import run_on_executor, run_callback
from common.zkOpers import Requests_ZkOpers
from backup_utils.dispath_backup_worker import DispatchBackupWorker
from backup_utils.backup_worker import BackupWorkers
from backup_utils.base_backup_check import get_response_request, get_local_backup_status


@require_basic_auth
class Backup(APIHandler):
    """ /backup 接口路由入口类。当mcluster /backup 接口收到post请求后，在
    zookeeper里记录备份开始的信息，并向具体的节点分发备份任务。

    Usage:
        full backup:
            curl --user root:root -d "backup_type=full" "http://127.0.0.1:8888/backup"
        incremental backup:
            curl --user root:root -d "backup_type=incr" "http://127.0.0.1:8888/backup"
    """
    def post(self):
        incr_basedir = self.get_argument("incr_basedir", None)
        backup_type = self.get_argument("backup_type")
        if not backup_type:
            raise HTTPAPIErrorException("backup params is not given, please check 'backup_type' params.", status_code=417)

        zkOper = Requests_ZkOpers()
        zkOper.write_backup_backup_info({"backup type":"backup is building"})
        worker = DispatchBackupWorker(backup_type, incr_basedir)
        worker.start()

        result = {}
        result.setdefault("message", "backup process is running, please waiting")
        self.finish(result)


# eg. curl --user root:root -d "backup_type=full" "http://127.0.0.1:8888/inner/backup"
@require_basic_auth
class Inner_Backup_Action(APIHandler):

    def post(self):
        backup_type = self.get_argument("backup_type")
        incr_basedir = self.get_arguments("incr_basedir")

        if not backup_type:
            raise HTTPAPIErrorException("backup params is not transmit, please check 'backup_type' params.", status_code=417)

        backup_worker = BackupWorkers(backup_type, incr_basedir)
        backup_worker.start()
        result = {}
        result.setdefault("message", "inner backup process is running, please waiting")
        self.finish(result)


#eq curl  "http://127.0.0.1:8888/backup/check"
class BackUpCheck(APIHandler):

    @asynchronous
    @engine
    def get(self):
        return_result = yield self._check_backup_stat()
        self.finish(return_result)

    @run_on_executor()
    @run_callback
    def _check_backup_stat(self):
        zkOper = self.retrieve_zkOper()
        backup_info = zkOper.retrieve_backup_status_info()
        result = {}

        if not backup_info:
            raise HTTPAPIErrorException("this cluster is not backup, please full backup!", status_code=417)

        if 'backup type' in backup_info and 'backup is building' == backup_info['backup type']:
            result.setdefault("message", 'backup is processing')
            return result

        if 'recently_backup_ip: ' not in backup_info:
            raise HTTPAPIErrorException("last time backup is not successed", status_code=417)

        last_backup_ip = backup_info['recently_backup_ip: ']
        last_backup_time = backup_info['time: ']
        last_backup_type = {'backup_type': backup_info['backup_type: ']}

        time = long(datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
        backup_time = long(datetime.datetime.strptime(last_backup_time, "%Y-%m-%d %H:%M:%S").strftime('%Y%m%d%H%M%S'))

        url_post = "/backup/checker"

        if (time - backup_time) / 10000 > 30:
            result.setdefault("message", "last backup expired")

        else:
            response_message = get_response_request(last_backup_ip, url_post, last_backup_type)
            logging.info(response_message)

            if response_message["meta"]["code"] == 417:
                result.setdefault("message", "inner backup interface params transmit failed")

            elif response_message["meta"]["code"] == 200:
                message = response_message['response']
                if -1 != message['message'].find('success'):
                    result.setdefault("message", '%s backup success, backup ip: %s' %(last_backup_type['backup_type'], last_backup_ip))
                if -1 != message['message'].find('starting'):
                    result.setdefault("message", 'last time %s backup is processing, backup ip: %s' %(last_backup_type['backup_type'], last_backup_ip))
                else:
                    result.setdefault("message", 'last time %s backup failed, backup ip: %s' %(last_backup_type['backup_type'], last_backup_ip))
            else:
                result.setdefault("message", 'last time %s backup failed, backup ip: %s' %(last_backup_type['backup_type'], last_backup_ip))

        return result


#eq curl -d "backup_type=full" "http://localhost:8888/backup/checker"
class BackUp_Checker(APIHandler):

    def post(self):
        zkOper = self.retrieve_zkOper()
        backup_type = self.get_argument("backup_type")
        if not backup_type:
            raise HTTPAPIErrorException("backup_type params is not given, please check 'backup_type' params.", status_code=417)

        result = {"message": "%s backup is starting" %backup_type}
        backup_info = zkOper.retrieve_type_backup_status_info(backup_type)

        # 不要问我从哪里来，我的故乡在远方。翻翻历史便可知。
        _tmp_type = 'backup' if backup_type == 'full' else 'incr_backup'
        backup_start_time = backup_info['{tmp_type}_start_time:'.format(tmp_type=_tmp_type)]
        backup_status = backup_info['{tmp_type}_status:'.format(tmp_type=_tmp_type)]
        backup_time = datetime.datetime.strptime(backup_start_time, "%Y-%m-%d %H:%M:%S").strftime('%Y%m%d%H%M%S')
        local_backup_result = get_local_backup_status(backup_type, backup_time)
        _msgs = {
            'backup_succecced': "{backup_type} backup success",
            'backup_starting': "{backup_type} backup is starting",
            'backup_failed': "{backup_type} backup is failed"
        }

        if local_backup_result:
            result["message"] = _msgs[backup_status].format(backup_type=backup_type)

        self.finish(result)

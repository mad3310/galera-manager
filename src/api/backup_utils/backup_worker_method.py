'''
Created on 01.11, 2016

@author: xu
'''

from tornado import escape
from backup_utils.base_backup_worker import BaseBackupWorker
from common.zkOpers import Requests_ZkOpers
from common.utils.exceptions import UserVisiableException

class BackupWorkerMethod(BaseBackupWorker):

    zkOper = Requests_ZkOpers()
    
    def _get_usable_ips(self):
        online_node_list = self.zkOper.retrieve_started_nodes()
        
        try:
            system_loads = self._retrieve_system_load(online_node_list)
            available_spaces = self._retrieve_available_space_for_disk(online_node_list)
            available_memory = self._retrieve_available_memory(online_node_list)       
        except Exception, e:
            raise UserVisiableException(e)
        
        usable_ips = self._analysis_usable_backup_node(system_loads, available_spaces, available_memory)
        return usable_ips
        

    def _retrieve_system_load(self, online_node_list):
        url_path = "/node/stat/workload"
        response_message_info = self._dispatch_request(online_node_list, 'GET', url_path)
        result = {}
        for _info in response_message_info:
            if response_message_info[_info]["meta"]["code"] == 200:
                last_message = escape.json_decode(response_message_info[_info]['response'])
                result.setdefault(_info, last_message)
        return result

    def _retrieve_available_space_for_disk(self, online_node_list):
        url_path = "/node/stat/datadir/available"
        response_message_info = self._dispatch_request(online_node_list, 'GET', url_path)
        result = {}
        for _info in response_message_info:
            if response_message_info[_info]["meta"]["code"] == 200:
                last_message = escape.json_decode(response_message_info[_info]['response'])
                result.setdefault(_info, last_message)
        return result
    
    def _retrieve_available_memory(self, online_node_list):
        url_path = "/node/stat/memory/available"
        response_message_info = self._dispatch_request(online_node_list, 'GET', url_path)
        result = {}
        for _info in response_message_info:
            if response_message_info[_info]["meta"]["code"] == 200:
                last_message = escape.json_decode(response_message_info[_info]['response'])
                result.setdefault(_info, last_message)
        return result


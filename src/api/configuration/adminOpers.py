'''
Created on Apr 25, 2015

@author: root
'''
import json
import logging
from tornado.options import options

from common.helper import get_localhost_ip
from common.zkOpers import ZkOpers
from common.configFileOpers import ConfigFileOpers

class AdminOpers(object):
    
    def sync_info_from_zk(self, ip):
        zkOper = ZkOpers()
    
        try:
            cluster_existed = zkOper.existCluster()
            if cluster_existed:
                clusterUUID = zkOper.getClusterUUID() 
                data,_ = zkOper.retrieveClusterProp(clusterUUID)
                
                #node_ip_addr = get_localhost_ip()
                node_ip_addr = ip
                return_result = zkOper.retrieve_data_node_info(node_ip_addr)
                
                json_str_data = data.replace("'", "\"")
                dict_data = json.loads(json_str_data)
                if type(return_result) is dict and type(dict_data) is dict:
                    config_file_obj = ConfigFileOpers()
                    config_file_obj.setValue(options.data_node_property, return_result)
                    config_file_obj.setValue(options.cluster_property, dict_data)
                    logging.debug("program has re-written zk data into configuration file")
                else:
                    logging.info("write data into configuration failed")
        finally:
            zkOper.stop()
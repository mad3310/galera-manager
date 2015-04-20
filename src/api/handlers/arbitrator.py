#-*- coding: utf-8 -*-
import re
import tornado

from common.invokeCommand import InvokeCommand
from base import APIHandler
from tornado.options import options
from common.utils.exceptions import HTTPAPIError
from common.helper import  get_localhost_ip
from tornado.web import asynchronous
from common.configFileOpers import ConfigFileOpers


# check whether the name of this node is end the -n-2.
#eg curl "http://localhost:8888/inner/arbitrator/ip"

class ArbitratorIP(APIHandler):
    
    confOpers = ConfigFileOpers()
    
    @tornado.gen.engine
    @asynchronous
    def get(self):
        ret_dict = self.confOpers.getValue(options.data_node_property, ['dataNodeName','dataNodeIp'])
        node_name = ret_dict['dataNodeName']
        obj = re.search("-n-2", node_name)
        if obj != None:
            self.finish(ret_dict['dataNodeIp'])
        else:
            self.finish("false")


class ArbitratorStart(APIHandler):
    
    confOpers = ConfigFileOpers()
    
    @tornado.gen.engine
    @asynchronous
    def get(self):
        data_node_list = self.zkOper.retrieve_data_node_list()
        local_ip = get_localhost_ip()
        
        data_node_list.remove(local_ip)
        list_len = str(data_node_list)
        if list_len < 2:
            raise HTTPAPIError(status_code=500, failed_detail = "cluster nodes is less than3",
                                       notification = "direct",
                                       log_message = "cluster nodes is less than 3",
                                       response = "cluster nodes is less than 3")

        cluster_name = self.confOpers.getValue(options.cluster_property,['clusterName'])
        cmd = "garbd -a %s:4567,%s:4567 -g %s -l /var/log/garbd.log -d" %(data_node_list[0], data_node_list[1], cluster_name)
        invokeCommand = InvokeCommand()
        ret_sub_p =  invokeCommand._runSysCmdnoWait(cmd)
        if ret_sub_p == False:
            raise HTTPAPIError("Garbd arbitrator start failed", \
                            notification = "direct", \
                            log_message="garbd arbitrator start failed",
                            response = "garbd arbitrator start failed")

        self.finish('true')

class ArbitratorStatusCheck(APIHandler):
    
    @tornado.gen.engine
    @asynchronous
    def get(self):
        ret_dict = self.confOpers.getValue(options.data_node_property, ['dataNodeName','dataNodeIp'])
        node_name = ret_dict['dataNodeName']
        obj = re.search("-n-2", node_name)
        if obj != None:
            cmd = "ps -ef |pgrep 'garbd' |wc -l"
            invokeCommand = InvokeCommand()
            count = invokeCommand._runSysCmd(cmd) 
            if count == 1:
                self.finish("true")
            else:
                self.finish("false")
        else:
            self.finish("false")

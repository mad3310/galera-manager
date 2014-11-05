#-*- coding: utf-8 -*-

import tornado.httpclient
import kazoo
import logging

from common.configFileOpers import ConfigFileOpers
from common.tornado_basic_auth import require_basic_auth
from base import APIHandler
from tornado.options import options
from common.invokeCommand import InvokeCommand
from common.helper import issue_mycnf_changed
from common.node_mysql_service_opers import Node_Mysql_Service_Opers
from common.utils.exceptions import HTTPAPIError
from common.node_stat_opers import NodeStatOpers

# add data node into mcluster
# eg. curl --user root:root -d "dataNodeIp=192.168.0.20&dataNodeName=letv_mcluster_test_1_node_2" "http://localhost:8888/cluster/node"
@require_basic_auth
class AddDataNodeToMCluster(APIHandler):
    
    confOpers = ConfigFileOpers()
    
    def post(self):
        try:
            requestParam = {}
            args = self.request.arguments
            logging.info("args :" + str(args))
            for key in args:
                value = args[key][0]
                requestParam.setdefault(key,value)
            
            if requestParam != {}:
                self.confOpers.setValue(options.data_node_property, requestParam)
            
            dataNodeProprs = self.confOpers.getValue(options.data_node_property)
            clusterUUID = self.zkOper.getClusterUUID()
            self.zkOper.writeDataNodeInfo(clusterUUID, dataNodeProprs)
        
            data,stat = self.zkOper.retrieveClusterProp(clusterUUID)
            self.confOpers.setValue(options.cluster_property, eval(data))
        
            fullText,stat = self.zkOper.retrieveMysqlProp(clusterUUID)
            self.confOpers.writeFullText(options.mysql_cnf_file_name, fullText)
        
            data_node_ip = requestParam.get('dataNodeIp')
            mycnfParam = self.confOpers.getValue(options.mysql_cnf_file_name)
            orginal_cluster_address = mycnfParam['wsrep_cluster_address']
            
            
            index = orginal_cluster_address.find("//")
            
            ip_str = orginal_cluster_address[index + 2 :]
            ip_lists = ip_str.rstrip().split(",")
            dict = {}
            
            if data_node_ip not in ip_lists:
                new_cluster_address = orginal_cluster_address + "," + str(data_node_ip)
                
                data_node_name = requestParam.get('dataNodeName')
        
                mysql_cnf_full_text = self.confOpers.retrieveFullText(options.mysql_cnf_file_name)
                #self.confOpers.writeFullText(options.mysql_cnf_file_name, mysql_cnf_full_text)
        
                keyValueMap = {}
                keyValueMap.setdefault('wsrep_cluster_address',new_cluster_address)
                keyValueMap.setdefault('wsrep_node_name', str(data_node_name))
                keyValueMap.setdefault('wsrep_node_address' ,str(data_node_ip))
                self.confOpers.setValue(options.mysql_cnf_file_name, keyValueMap)
        
                mysql_cnf_full_text = self.confOpers.retrieveFullText(options.mysql_cnf_file_name)
                self.zkOper.writeMysqlCnf(clusterUUID, mysql_cnf_full_text, issue_mycnf_changed)
            else:
                dict.setdefault("message", "node already in the cluster")
                pass
        except Exception,e:
            logging.error(e)
            error_message="server error in cluster_node add"
            raise HTTPAPIError(status_code=500, error_detail= error_message,\
                                    notification = "direct", \
                                    log_message= error_message,\
                                    response =  error_message)
#        dict.setdefault("code", "000000")
        dict.setdefault("message", "add data node into cluster successful!")
        self.finish(dict)
        
        
# sync data node info from zk
# eg. curl "http://localhost:8888/node/sync"
class SyncDataNode(APIHandler):
    
    confOpers = ConfigFileOpers()
    
    def get(self,ip_address):
        try:    
            if ip_address is None:
                error_message="you should specify the ip address need to sync"
                raise HTTPAPIError(status_code=500, error_detail= error_message,\
                                    notification = "direct", \
                                    log_message= error_message,\
                                    response =  error_message)        
            return_result = self.zkOper.retrieve_data_node_info(ip_address)
            self.confOpers.setValue(options.data_node_property, return_result)
        except Exception,e:
            logging.error(e)
            error_message="Specify the ip address "
            raise HTTPAPIError(status_code=400, error_detail= error_message,\
                                    notification = "direct", \
                                    log_message= error_message,\
                                    response =  error_message)
        dict = {}
#        dict.setdefault("code", "000000")
        dict.setdefault("message", "sync data node info to local successful!")
        self.finish(dict)
        
# check data node if there are some errors in log
# eg. curl "http://localhost:8888/inner/node/check/log/error"
class DataNodeMonitorLogError(APIHandler):
    
    invokeCommand = InvokeCommand()
    
    def get(self):
        result = self.invokeCommand.run_check_shell(options.check_datanode_error)
        self.finish(result)
        
# check data node if there are some warnings in log
# eg. curl "http://localhost:8888/inner/node/check/log/warning"
class DataNodeMonitorLogWarning(APIHandler):
    
    invokeCommand = InvokeCommand()
    
    def get(self):
        result = self.invokeCommand.run_check_shell(options.check_datanode_warning)
        self.finish(result)
        
        
# check data node if there are some information of shutting down in log
# eg. curl "http://localhost:8888/inner/node/check/log/health"
class DataNodeMonitorLogHealth(APIHandler):
    
    invokeCommand = InvokeCommand()
    
    def get(self):
        result = self.invokeCommand.run_check_shell(options.check_datanode_health)
        self.finish(result)
        
# start mysqld service on data node
# eg. curl --user root:root "http://localhost:8888/node/start"   start one node which has configed the all node ip list on my.cnf
# eg. curl --user root:root -d "isNewCluster=True" "http://localhost:8888/node/start"   start new cluster on this node
@require_basic_auth
class DataNodeStart(APIHandler):
    
    mysql_service_opers = Node_Mysql_Service_Opers()
    
    @tornado.web.asynchronous
    def post(self):
        try:
            isNewCluster = False
        
            args = self.request.arguments
            logging.info("args :" + str(args))
            if args != {}:
                isNewCluster = args['isNewCluster'][0]
        
            self.mysql_service_opers.start(isNewCluster)
        except Exception,e:
            logging.error(e)
            error_message="server error in cluster_node start"
            raise HTTPAPIError(status_code=500, error_detail= error_message,\
                                    notification = "direct", \
                                    log_message= error_message,\
                                    response =  error_message)
        dict = {}
        dict.setdefault("message", "due to start data node need a large of times, please wait to finished and email to you, when data node has started!")
#        dict.setdefault("code", "000000")
        self.finish(dict)

# stop mysqld service on data node
# eg. curl --user root:root "http://localhost:8888/node/stop"
@require_basic_auth
class DataNodeStop(APIHandler):
    
    mysql_service_opers = Node_Mysql_Service_Opers()
    
    @tornado.web.asynchronous
    def get(self):
        try:
            self.mysql_service_opers.stop()
        except Exception,e:
            logging.error(e)
            error_message="server error in cluster_node stop"
            raise HTTPAPIError(status_code=500, error_detail= error_message,\
                                    notification = "direct", \
                                    log_message= error_message,\
                                    response =  error_message)
        dict = {}
#        dict.setdefault("code", "000000")
        dict.setdefault("message", "due to stop data node need a large of times, please wait to finished and email to you, when data node has stoped!")
        
        self.finish(dict)
        
# retrieve node stat 
# eg. curl "http://localhost:8888/node/stat"       
class DataNodeStat(APIHandler):
    
    stat_opers = NodeStatOpers()
    
    def get(self):
        return_dict = self.stat_opers.stat()
        self.finish(return_dict)
        
        
# retrieve the node stat for data dir size
# eg. curl "http://localhost:8888/node/stat/datadir/size"        
class StatDataDirSize(APIHandler):
    stat_opers = NodeStatOpers()
    
    def get(self):
        return_dict = self.stat_opers.stat_data_dir_size()
        self.finish(return_dict)
        
# retrieve the node stat for mysql cpu partion
# eg. curl "http://localhost:8888/node/stat/mysqlcpu/partion"        
class StatMysqlCpuPartion(APIHandler):
    stat_opers = NodeStatOpers()
    
    def get(self):
        return_dict = self.stat_opers.stat_mysql_cpu()
        self.finish(return_dict)
        
        
# retrieve the node stat for mysql memory partion
# eg. curl "http://localhost:8888/node/stat/mysqlmemory/partion"        
class StatMysqlMemoryPartion(APIHandler):
    stat_opers = NodeStatOpers()
    
    def get(self):
        return_dict = self.stat_opers.stat_mysql_memory()
        self.finish(return_dict)
        
        
# retrieve the node stat for memory size
# eg. curl "http://localhost:8888/node/stat/memory/size"        
class StatNodeMemorySize(APIHandler):
    stat_opers = NodeStatOpers()
    
    def get(self):
        return_dict = self.stat_opers.stat_node_memory()
        self.finish(return_dict)
        

# no used
class CopyConfigFileInfoHandler(APIHandler):
    def get(self):
        ip = self.get_argument("ip", None)
        
        #access to the target ip machine to retrieve the dict,then modify the config
        http_client = tornado.httpclient.AsyncHTTPClient()
        
        try:
            requesturi = "http://"+ip+":"+str(options.port)+"/retrieveConfigInfo"
            http_client.fetch(requesturi,self.handleResponse)
        except tornado.httpclient.HTTPError as e:
            print "Error:", e
            
        http_client.close()
        
        dict = {}
        dict.setdefault("message", "change config file info is ok!")
        self.finish(dict)
            
        
    def handleResponse(self,response):
        resultValue = {}
        if response.error:
            print "Error:", response.error
        else:
            resultValue = response.body
            print resultValue
            if resultValue == {}:
                print "change config file error!"
            else:
                s = ConfigFileOpers()
                s.setValue(options.mysql_cnf_file_name, eval(resultValue))
  
# retrieve the port of the  node.
# eg. curl "http://localhost:8888/inner/node_port/check"
class PortCheck(APIHandler):
    invokeCommand = InvokeCommand()
    def get(self):
        check_port_shell = 'netstat -anutlp | grep 3306|grep -w LISTEN'
        result = self.invokeCommand.run_check_shell(check_port_shell)
        if result != '':
            self.finish('true')
        else:
            self.finish('false')               

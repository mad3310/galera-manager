# -*- coding: utf-8 -*-

import logging
import uuid
import kazoo

from tornado.options import options
from tornado.web import asynchronous

from common.configFileOpers import ConfigFileOpers
from common.invokeCommand import InvokeCommand
from common.tornado_basic_auth import require_basic_auth
from common.cluster_mysql_service_opers import Cluster_Mysql_Service_Opers
from common.utils.exceptions import HTTPAPIErrorException
from common.utils import CLUSTER_NAME
from base import APIHandler


'''
Created on 2013-7-21

@author: asus
'''

# create mcluster
# eg. curl --user root:root -d "clusterName=letv_mcluster_test_1&dataNodeIp=192.168.0.10&dataNodeName=letv_mcluster_test_1_node_1" "http://localhost:8888/cluster"
@require_basic_auth
class CreateMCluster(APIHandler):

    confOpers = ConfigFileOpers()

    def post(self):
        #check if exist cluster
        zkOper = self.retrieve_zkOper()
        existCluster = zkOper.existCluster()
        if existCluster:
            if zkOper.judgeClusterStatus("remove"):
                zkOper.remove_zk_info(CLUSTER_NAME)
            else:
                raise HTTPAPIErrorException("server has belong to a cluster,should be not create new cluster!", status_code=417)


        requestParam = {}
        args = self.request.arguments
        logging.info("args :" + str(args))

        if not args:
            raise HTTPAPIErrorException("params is empty")

        for key in args:
            value = args[key][0]
            requestParam.setdefault(key, value)

        if "clusterName" not in requestParam or 'dataNodeName' not in requestParam:
            raise HTTPAPIErrorException("cluster_name or node_name is empty, please check it!")

        if 'dataNodeIp' not in requestParam:
            raise HTTPAPIErrorException("node_ip is empty, please check it!")


        cluster_name = requestParam['clusterName']
        if len(cluster_name) >= 33:
            raise HTTPAPIErrorException("Cluster name is too long, please use name whoes length is less than 33 characters",
                                        status_code=417)

        if self.confOpers.ipFormatChk(requestParam['dataNodeIp']):
            raise HTTPAPIErrorException("dataNodeIp is illegal", status_code=417)

        clusterUUID = CLUSTER_NAME + '/' + str(uuid.uuid1())
        requestParam.setdefault("clusterUUID",clusterUUID)

        if requestParam != {}:
            self.confOpers.setValue(options.cluster_property, requestParam)
            self.confOpers.setValue(options.data_node_property, requestParam)

        clusterProps = self.confOpers.getValue(options.cluster_property)
        dataNodeProprs = self.confOpers.getValue(options.data_node_property)
        zkOper.writeClusterInfo(clusterUUID, clusterProps)
        zkOper.writeDataNodeInfo(clusterUUID, dataNodeProprs)

        result = {}
#        dict.setdefault("code", '000000')
        result.setdefault("message", "creating cluster successful!")
        self.finish(result)

# init mcluster
# eg. curl --user root:root "http://localhost:8888/cluster/init?forceInit=false"
@require_basic_auth
class InitMCluster(APIHandler):

    confOpers = ConfigFileOpers()

    invokeCommand = InvokeCommand()

    def get(self):
        args = self.request.arguments
        forceInit = args['forceInit'][0]

        isLock = False
        lock = None

        try:
            zkOper = self.retrieve_zkOper()
            #existCluster = zkOper.existCluster()

            isLock,lock = zkOper.lock_init_node_action()

            dataNodeProKeyValue = self.confOpers.getValue(options.data_node_property, ['dataNodeIp','dataNodeName'])
            data_node_ip = dataNodeProKeyValue['dataNodeIp']
            data_node_name = dataNodeProKeyValue['dataNodeName']

            clusterProKeyValue = self.confOpers.getValue(options.cluster_property, ['clusterUUID','clusterName'])
            clusterUUID = clusterProKeyValue['clusterUUID']

            #check if cluster has odd data node
            if not forceInit:
                dataNodeNumber = zkOper.getDataNodeNumber(clusterUUID)
                if dataNodeNumber/2 == 0:
                    raise HTTPAPIErrorException("the number should be not odd number,please add 1 or 3 data node into cluster!",
                                                status_code=417)

            clusterName = clusterProKeyValue['clusterName']
            clusterAddress = 'gcomm://%s' % (data_node_ip)

            requestParam = {'wsrep_cluster_name':clusterName, 'wsrep_node_address':data_node_ip, 'wsrep_cluster_address':clusterAddress, 'wsrep_node_name':data_node_name}
            self.confOpers.setValue(options.mysql_cnf_file_name, requestParam)

            sst_user_password = self.invokeCommand.runBootstrapScript()

            mysql_cnf_text = self.confOpers.retrieveFullText(options.mysql_cnf_file_name)
            zkOper.writeMysqlCnf(clusterUUID, mysql_cnf_text)
            zkOper.write_started_node(data_node_ip)
        except kazoo.exceptions.LockTimeout:
            raise HTTPAPIErrorException("the mysql cluster is initing,please wait for the completion of other machine join this cluster.",
                                        status_code=417)

        finally:
            if isLock:
                zkOper.unLock_init_node_action(lock)


        result = {}
#        dict.setdefault("code", '000000')
        result.setdefault("message", "init cluster successful!")
        result.setdefault("sst_user_password", sst_user_password)
        self.finish(result)

# sync mcluster
# eg. curl "http://localhost:8888/cluster/sync"
class SyncMCluster(APIHandler):

    confOpers = ConfigFileOpers()

    def get(self):

        zkOper = self.retrieve_zkOper()
        existCluster = zkOper.existCluster()

        clusterUUID = zkOper.getClusterUUID()
        data, _ = zkOper.retrieveClusterProp(clusterUUID)
        self.confOpers.setValue(options.cluster_property, eval(data))

        result = {}
#       dict.setdefault("code", '000000')
        result.setdefault("message", "sync mcluster info to local successful!")
        self.finish(result)



# start mysqld service on every data node for entity cluster
# eg. curl --user root:root -d "cluster_flag=new" "http://localhost:8888/cluster/start"
@require_basic_auth
class ClusterStart(APIHandler):

    mysql_service_opers = Cluster_Mysql_Service_Opers()

    @asynchronous
    def post(self):
        args = self.request.arguments

        requestParam = {}
        for key in args:
            requestParam.setdefault(key,args[key][0])

        cluster_flag = requestParam.get('cluster_flag')

        if cluster_flag != 'new' and cluster_flag != 'old':
            raise HTTPAPIErrorException("arguments are wrong, retry again.", status_code=417)

        try:
            self.mysql_service_opers.start(cluster_flag, None)
        except kazoo.exceptions.LockTimeout:
            raise HTTPAPIErrorException("current operation is using by other people, please wait a moment to try again!",
                                        status_code=578)

        result = {}
#       dict.setdefault("code", '000000')
        result.setdefault("message", "due to start cluster need a large of times, please wait to finished and email to you, when cluster have started!")
        self.finish(result)

# cluster status. Return information about the cluster status.
#eq curl --user root:root "http://localhost:8888/cluster/check/online_node"
@require_basic_auth
class ClusterStatus(APIHandler):

    @asynchronous
    def get(self):
        try:
            zkOper = self.retrieve_zkOper()
            existCluster = zkOper.existCluster()
            cluster_status = zkOper.retrieveClusterStatus()
            cluster_started_nodes = zkOper.retrieve_started_nodes()
        except kazoo.exceptions.LockTimeout:
            raise HTTPAPIErrorException("current operation is using by other people, please wait a moment to try again!",
                                        status_code=578)

        result = {}
#       dict.setdefault("code", '000000')
        result['message'] = cluster_status['_status']
        result['nodelist'] = cluster_started_nodes

        self.finish(result)



# stop mysqld service on every data node for entity cluster
# eg. curl --user root:root "http://localhost:8888/cluster/stop"
@require_basic_auth
class ClusterStop(APIHandler):

    mysql_service_opers = Cluster_Mysql_Service_Opers()

    @asynchronous
    def get(self):
        try:
            self.mysql_service_opers.stop()
        except kazoo.exceptions.LockTimeout:
            raise HTTPAPIErrorException("current operation is using by other people, please wait a moment to try again!",
                                        status_code=578)

        status_dict = {}
        status_dict['_status'] = 'stopping'

        zkOper = self.retrieve_zkOper()
        existCluster = zkOper.existCluster()

        zkOper.writeClusterStatus(status_dict)

        result = {}
        #dict.setdefault("code", '000000')
        result.setdefault("message", "due to stop cluster need a large of times, please wait to finished and email to you, when cluster have stoped!")
        self.finish(result)



# eg. curl --user root:root "http://localhost:8888/cluster/zk/remove"
@require_basic_auth
class ClusterZkRemove(APIHandler):

    @asynchronous
    def get(self):
        status_dict = {}
        status_dict['_status'] = 'remove'

        zkOper = self.retrieve_zkOper()

        if zkOper.existCluster():
            zkOper.writeClusterStatus(status_dict)
        result = {}
        result.setdefault("message", "del zk info success")
        self.finish(result)


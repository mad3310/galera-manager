import base64
import logging

from common.zkOpers import ZkOpers
from tornado.options import options
from common.configFileOpers import ConfigFileOpers
from tornado.httpclient import HTTPClient
from tornado.httpclient import HTTPError
from common.invokeCommand import InvokeCommand

from common.configFileOpers import ConfigFileOpers

confOpers = ConfigFileOpers()

def issue_mycnf_changed(self):
    keyList = []
    keyList.append('wsrep_cluster_address')
    
    zkOper = ZkOpers()
    
    try:
        clusterUUID = zkOper.getClusterUUID()
        sourceText,_ = zkOper.retrieveMysqlProp(clusterUUID, issue_mycnf_changed)
    finally:
        zkOper.stop()
    
    keyValueDict = getDictFromText(sourceText, keyList)
    confOpers.setValue(options.mysql_cnf_file_name, keyValueDict)
    
    
def retrieve_kv_from_db_rows(rows, key_list=None):
    key_value = {}
    
    if rows == None:
        return key_value
    
    if key_list == None:
        return key_value
    
    for i in range(len(rows)):
        key,value = rows[i]
        for j in range(len(key_list)):
            if key_list[j] == key:
                key_value.setdefault(key,value)
    
    return key_value


def _request_fetch(request):
    #access to the target ip machine to retrieve the dict,then modify the config
    http_client = HTTPClient()
    
    response = None
    try:
        response = http_client.fetch(request)
    except HTTPError, e:
        logging.error(e)
    
    return_result = False
    if response != None:    
        if response.error:
            return_result = False
            message = "remote access,the key:%s,error message:%s" % (request,response.error)
            logging.error(message)
        else:
            return_result = response.body.strip()
            
    http_client.close()
    logging.info("coming here mean Exception was caught, if exist any")        
    return return_result
    
    
def _retrieve_userName_passwd():
    confDict = confOpers.getValue(options.cluster_property, ['adminUser','adminPassword'])
    adminUser = confDict['adminUser']
    adminPasswd = base64.decodestring(confDict['adminPassword'])
    return (adminUser,adminPasswd)

def getDictFromText(sourceText, keyList):
    totalDict = {}
    resultValue = {}
    
    lineList = sourceText.split('\n')
    for line in lineList:
        if not line:
            continue
        
        pos1 = line.find('=')
        key = line[:pos1]
        value = line[pos1+1:len(line)].strip('\n')
        totalDict.setdefault(key,value)
        
    if keyList == None:
        resultValue = totalDict
    else:
        for key in keyList:
            value = totalDict.get(key)
            resultValue.setdefault(key,value)
            
    return resultValue

def check_leader():
    invokeCommand = InvokeCommand()
    zk_address, zk_port = get_zk_address()
    cmd = "echo stat |nc %s %s| grep Mode" %(zk_address, zk_port)
    ret_str, _ = invokeCommand._runSysCmd(cmd)
    invokeCommand = None
    if ret_str.find('leader') == -1:
        return False
    return True

def is_monitoring(host_ip=None, zkOper=None):
    try:
        stat = zkOper.retrieveClusterStatus()
        logging.info("is_monitoring: stat: %s, host_ip: %s" % (str(stat), str(host_ip)))
        if not stat or '_status' not in stat:
            return False
        elif stat.get('_status') == 'initializing' and ( not host_ip or host_ip not in zkOper.retrieve_started_nodes() ):
            return False
        return True
    except:
        logging.info("is_monitoring: except False")
        return False

def get_localhost_ip():
    cmd="""ifconfig $(route -n|grep '^0.0.0.0'|awk '{print $NF}')|awk '/inet addr/,gsub("addr:",""){print $2}'"""
    invokeCommand = InvokeCommand()
    ret_str, _ = invokeCommand._runSysCmd(cmd)
    invokeCommand = None
    return ret_str

def get_zk_address():
    ret_dict = confOpers.getValue(options.zk_address, ['zkAddress','zkPort'])
    zk_address = ret_dict['zkAddress']
    zk_port = ret_dict['zkPort']
    
    if "" == zk_port:
        zk_port = "2181"
        
    return zk_address ,zk_port

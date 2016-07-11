import base64
import logging
import os
from tornado.options import options
from tornado.httpclient import HTTPClient
from tornado.httpclient import HTTPError
from common.invokeCommand import InvokeCommand
from common.utils import local_get_zk_address

from common.configFileOpers import ConfigFileOpers

confOpers = ConfigFileOpers()

    
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


def _request_fetch(request, timeout=20):
    #access to the target ip machine to retrieve the dict,then modify the config
    http_client = HTTPClient()
    
    response = None

    request.request_timeout = timeout
        
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

def check_leader(zk):
    request_param = {"zkLeader" : "True"}
    ret_dict = confOpers.getValue(options.mcluster_manager_cnf, request_param)
    zkLeader = ret_dict.get('zkLeader')
    if zkLeader:
        if zkLeader=='True':
            return True
        else:
            return False
    
    if zk.command("srvr").find('leader') == -1:
        request_param["zkLeader"] = "False"

    with open(options.mcluster_manager_cnf,'a') as f:
        f.writelines('zkLeader=' + request_param['zkLeader'])

    zkLeader = request_param.get("zkLeader")
    if zkLeader == 'True':
        return True
    else:
        return False

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

def retrieve_monitor_password():
    value = confOpers.getValue(options.mysql_cnf_file_name)["wsrep_sst_auth"]
    _password = value.split(":")[1][:-1]
    return _password

def retrieve_directory_available(directory):
    _vfs = os.statvfs(directory)
    _disk_available = _vfs.f_bavail * _vfs.f_bsize / (1024.0*1024*1024)
    return _disk_available
    
def retrieve_directory_capacity(directory):
    _vfs = os.statvfs(directory)
    _disk_capacity = _vfs.f_blocks * _vfs.f_bsize / (1024.0*1024*1024)
    return _disk_capacity


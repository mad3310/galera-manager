#-*- coding: utf-8 -*-

from common.configFileOpers import ConfigFileOpers
from common.invokeCommand import InvokeCommand
from base import APIHandler
from tornado.options import options
from common.utils.exceptions import HTTPAPIError

import MySQLdb
import json
import os
import re

'''
Created on 2013-9-16

@author: duanwei
'''
        
# start gbalancer
# eg. curl -d "user=monitor&passwd=7wupAol9&iplist_port=10.200.85.112:3306,10.200.85.110:3306,10.200.85.111:3306&port=3306" "http://localhost:8888/glb/start"
class Startgbalancer(APIHandler):
    
    confOpers = ConfigFileOpers()
    
    invokeCommand = InvokeCommand()

    def post(self):
        user = self.get_argument('user',None)
        passwd = self.get_argument('passwd',None)
        iplist_port = self.get_argument('iplist_port',None)
        port = self.get_argument('port',None)
        args = self.get_argument('args',None)

        if user is None or user == '':
            raise HTTPAPIError(status_code=417, error_detail="lost user argument",\
                                notification = "direct", \
                                log_message= "start GLB: lost user argument",\
                                response =  "please write user name.")
        if passwd is None or passwd == '':
            raise HTTPAPIError(status_code=417, error_detail="lost passwd argument",\
                                notification = "direct", \
                                log_message= "start GLB: lost passwd argument",\
                                response =  "please write password.")
        if iplist_port is None or iplist_port == '':
            raise HTTPAPIError(status_code=417, error_detail="lost iplist_port argument",\
                                notification = "direct", \
                                log_message= "start GLB: lost iplist_port argument",\
                                response =  "please write iplist and server port<ip:port>.")
        if port is None or port == '':
            raise HTTPAPIError(status_code=417, error_detail="lost port argument",\
                                notification = "direct", \
                                log_message= "start GLB: lost port argument",\
                                response =  "please write port.")
        if args is None:
            args = r'--daemon'
        
        #glb_config='{"User": "monitor","Pass": "1234567","Addr": "0.0.0.0","Port": "3306","Backend": ["10.200.86.14:3306","10.200.86.15:3306","10.200.86.16:3306"]}'
        glb_config={}
        glb_config['User'] = user
        glb_config['Pass'] = passwd
        glb_config['Addr'] = '0.0.0.0'
        glb_config['Port'] = port
        ips = iplist_port.split(",")
        glb_config['Backend'] = ips
        for ip in ips:
            i, p = ip.split(':')
            try:
                conn=MySQLdb.Connect(host=i,user=user,passwd=passwd,port=int(p))
            except Exception, e:
                conn=None
            if conn is None:
                raise HTTPAPIError(status_code=417, error_detail="cann't connect to mysql",\
                                   notification = "direct", \
                                   log_message= "cann't connect to mysql", \
                                   response = "please check mysqld or user password")
        if not os.path.exists(os.path.dirname(options.glb_json_file_name)):
            os.mkdir(os.path.dirname(options.glb_json_file_name))
        config_file = options.glb_json_file_name % (port)
        cmd = r'ps -ef|grep %s|grep -v grep' % (config_file)
        result = self.invokeCommand.run_check_shell(cmd).strip().split('\n')
        if not result[0] == '':
            raise HTTPAPIError(status_code=417, error_detail="this glb is running",\
                               notification = "direct", \
                               log_message= "this glb is running", \
                               response = "please check envirment")
        f = open(config_file, 'w')
        f.write(json.dumps(glb_config,sort_keys=True,indent=4))
        f.flush()
        f.close()

        self.invokeCommand.run_service_shell(options.start_gbalancer % (config_file, args))
        glb_proc = self.invokeCommand.run_check_shell(cmd).strip().split('\n')
        if len(glb_proc) != 1 and glb_proc[0] == '':
            raise HTTPAPIError(status_code=417, error_detail="glb start error",\
                               notification = "direct", \
                               log_message= "glb start error", \
                               response = "please check envirment")
        dict = {}
        #dict.setdefault("code", '000000')
        dict.setdefault("message", "start gbalancer successful!")
        self.finish(dict)
        
# stop gbalancer
# eg. curl -d "port=3306" "http://localhost:8888/glb/stop"
class Stopgbalancer(APIHandler):
    
    confOpers = ConfigFileOpers()
    
    invokeCommand = InvokeCommand()

    def post(self):
        port = self.get_argument('port',None)
        if port is None or port == '':
            raise HTTPAPIError(status_code=417, error_detail="lost port argument",\
                                notification = "direct", \
                                log_message= "start GLB: lost port argument",\
                                response =  "please write port.")
        cmd = r'netstat -ntlp|grep %s|grep -v grep' % (port)
        result = self.invokeCommand.run_check_shell(cmd).strip().split('\n')
        if result[0] == '':
            raise HTTPAPIError(status_code=417, error_detail="this glb is stopped",\
                               notification = "direct", \
                               log_message= "this glb is stopped", \
                               response = "please check envirment")

        temp = re.split('\s+', result[0])
        glb_pid = temp[len(temp)-1].split('/')[0]
        self.invokeCommand.run_check_shell(r'kill -9 %s' % (glb_pid))
        glb_proc = self.invokeCommand.run_check_shell(cmd).strip().split('\n')
        if not glb_proc[0] == '':
            raise HTTPAPIError(status_code=417, error_detail="cannot stop glb",\
                               notification = "direct", \
                               log_message= "cannot stop glb", \
                               response = "please check envirment")
        dict = {}
        #dict.setdefault("code", '000000')
        dict.setdefault("message", "stop gbalancer successful!")
        self.finish(dict)
        


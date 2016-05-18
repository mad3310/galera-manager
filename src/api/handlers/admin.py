#-*- coding: utf-8 -*-
import os
import stat
import base64
import shutil
import logging

from common.configFileOpers import ConfigFileOpers
from base import BaseHandler, APIHandler
from tornado.options import options
from common.utils.exceptions import HTTPAPIErrorException
from configuration.adminOpers import AdminOpers


# admin conf
# eg. curl -d "zkAddress=10.204.8.211&zkPort=2181" "http://localhost:8888/admin/conf"
class AdminConf(APIHandler):
    
    confOpers = ConfigFileOpers()
    
    adminOpers = AdminOpers()
    
    def post(self):
        requestParam = {}
        args = self.request.arguments
        for key in args:
            requestParam.setdefault(key,args[key][0])        
        
        if not requestParam:
            raise HTTPAPIErrorException("params is empty")

        #TODO:
        '''1.judge throwing code reuse rate is very low.
           2.thrown error code directly, automatically reads the type of error, error messages, and so on.'''

        if "zkAddress" not in requestParam or 'zkPort' not in requestParam:
            raise HTTPAPIErrorException("zkaddress or port is empty, please check it!")

        if self.confOpers.ipFormatChk(requestParam['zkAddress']):
            raise HTTPAPIErrorException("zkaddress is illegal", status_code=417)


        self.confOpers.setValue(options.mcluster_manager_cnf, requestParam)
        self.adminOpers.sync_info_from_zk(requestParam['zkAddress'][0])
            
        result = {}
#        dict.setdefault("code", '000000')
        result.setdefault("message", "admin conf successful!")
        self.finish(result)
        
        
        
# admin reset
# eg. curl --user root:root "http://localhost:8888/admin/reset"
class AdminReset(APIHandler):
    
    def get(self):
        template_path=os.path.join(options.base_dir, "templates")
        config_path = os.path.join(options.base_dir, "config")
    
        clusterPropTemFileName = os.path.join(template_path,"cluster.property.template")
        dataNodePropTemFileName = os.path.join(template_path,"dataNode.property.template")
        mclusterManagerCnfTemFileName = os.path.join(template_path,"mclusterManager.cnf.template")
    
        clusterPropFileName = os.path.join(config_path,"cluster.property")
        dataNodePropFileName = os.path.join(config_path,"dataNode.property")
        mclusterManagerCnfFileName = os.path.join(config_path,"mclusterManager.cnf")
        fileNameList = [clusterPropFileName,dataNodePropFileName,mclusterManagerCnfFileName]
    
        for fileName in fileNameList:
            if os.path.exists(fileName):
                os.chmod(fileName, stat.S_IWRITE)
                os.remove(fileName)
        
        shutil.copyfile(clusterPropTemFileName, clusterPropFileName)
        shutil.copyfile(dataNodePropTemFileName, dataNodePropFileName)
        shutil.copyfile(mclusterManagerCnfTemFileName, mclusterManagerCnfFileName)
   
        result = {}
        result.setdefault("message", "admin reset successful!")
        self.finish(result)
        
        
        
        
# create admin user
# eg. curl -d "adminUser=root&adminPassword=root" "http://localhost:8888/admin/user"
class AdminUser(APIHandler):
    
    confOpers = ConfigFileOpers()
    
    def post(self):
        requestParam = {}
        args = self.request.arguments
        logging.info("args :"+ str(args))
        if not args:
            raise HTTPAPIErrorException("params is empty")

        for key in args:
            value = args[key][0]
            if key == 'adminPassword':
                value = base64.encodestring(value).strip('\n')
            requestParam.setdefault(key,value)
            
        if "adminUser" not in requestParam or 'adminPassword' not in requestParam:
            raise HTTPAPIErrorException("admin user or password is empty, please check it!")

        self.confOpers.setValue(options.cluster_property, requestParam)
        
        result = {}
        #dict.setdefault("code", '000000')
        result.setdefault("message", "creating admin user successful!")
        self.finish(result)
#         
# no used        
# download cnf and property file, this is inner-API
class DownloadFile(BaseHandler):
    def get(self,filename):
        ifile  = open(filename, "r")
        self.set_header ('Content-Type', 'text/cnf')
        self.set_header ('Content-Disposition', 'attachment; filename='+filename+'')
        self.write (ifile.read())
        

        
#
#no used
#
class GenerateConfigFileHandler(APIHandler):
    def get(self):
        configFileContentText = self.render_string("my.cnf.template",ip_address='123.123.123.1,123.123.123.2,123.123.123.3',
                                                   cluster_instance_name='mcluster-instance');
                                                   
        # Write to the file
        f = file(options.mysql_cnf_file_name, 'w')
        f.write(configFileContentText)  
        f.close()
        
        del configFileContentText # remove the configFileContentText
        
        result = {}
        result.setdefault("message", "finished")
        
        self.finish(result)

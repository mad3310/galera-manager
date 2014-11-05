#-*- coding: utf-8 -*-

'''
Created on 2013-7-21

@author: asus
'''
import logging
from base import APIHandler
from common.tornado_basic_auth import require_basic_auth
from tornado.options import options
from common.dba_opers import DBAOpers
from common.configFileOpers import ConfigFileOpers
from common.utils import get_random_password
from common.utils.exceptions import HTTPAPIError

# create manager user in mcluster
# eg. curl --user root:root -d "role=manager&dbName=managerTest&userName=zbz" "http://localhost:8888/db/user"

# create readonly user in mcluster
# eg. curl --user root:root -d "role=ro&dbName=managerTest&userName=zbz" "http://localhost:8888/db/user"

# create write_read user in mcluster
# eg. curl --user root:root -d "role=wr&dbName=managerTest&userName=zbz" "http://localhost:8888/db/user"

# delete database user in mcluster
# eg. curl --user root:root -X DELETE "http://localhost:8888/Dbuser/{dbName}/{userName}/{ipAddress}"

# update database user parameter
# eg. 
# curl --user root:root -d "dbName=managerTest&userName=zbz&ip_address=127.0.0.1&max_queries_per_hour=200" "http://localhost:8888/db/user"
# curl --user root:root -d "dbName=managerTest&userName=zbz&ip_address=127.0.0.1&max_updates_per_hour=200" "http://localhost:8888/db/user"
# curl --user root:root -d "dbName=managerTest&userName=zbz&ip_address=127.0.0.1&max_connections_per_hour=200" "http://localhost:8888/db/user"
# curl --user root:root -d "dbName=managerTest&userName=zbz&ip_address=127.0.0.1&max_user_connections=200" "http://localhost:8888/db/user"


@require_basic_auth
class DBUser(APIHandler):
    
    dba_opers = DBAOpers()
    
    conf_opers = ConfigFileOpers()
    
    def post(self):
        dict = {}
        role = self.get_argument("role", None)
        dict.setdefault("role", role)
        dbName = self.get_argument("dbName", None)
        dict.setdefault("dbName " , dbName)
        userName = self.get_argument("userName", None)
        dict.setdefault("userName", userName)
        user_password = self.get_argument("user_password", None)
        ip_address = self.get_argument("ip_address", '%')
        dict.setdefault("ip_address " , ip_address)
        max_queries_per_hour = self.get_argument("max_queries_per_hour", 0)
        dict.setdefault("max_queries_per_hour", max_queries_per_hour)
        max_updates_per_hour = self.get_argument("max_updates_per_hour", 0)
        dict.setdefault("max_updates_per_hour", max_updates_per_hour)
        max_connections_per_hour = self.get_argument("max_connections_per_hour", 0)
        dict.setdefault("max_connections_per_hour", max_connections_per_hour)
        max_user_connections = self.get_argument("max_user_connections", 200)
        dict.setdefault("max_user_connections", max_user_connections)
        intg_dict = {}
        intg_dict.setdefault("args:", dict)
        logging.info(str(intg_dict))
        if role is None:
            raise HTTPAPIError(status_code=417, error_detail="when create db's user, no specify the user role",\
                                notification = "direct", \
                                log_message= "when create db's user, no specify the user role",\
                                response =  "please specify the user role.")
        
        if dbName is None:
            raise HTTPAPIError(status_code=417, error_detail="when create db's user, no specify the database name",\
                                notification = "direct", \
                                log_message= "when create db's user, no specify the database name",\
                                response =  "please specify the database name.")
        
        if userName is None:
            raise HTTPAPIError(status_code=417, error_detail="when create db's user, no specify the user name",\
                                notification = "direct", \
                                log_message= "when create db's user, no specify the user name",\
                                response =  "please specify the user name.")
        
        if ip_address is None:
            raise HTTPAPIError(status_code=417, error_detail="when create db's user, no specify the ip address",\
                                notification = "direct", \
                                log_message= "when create db's user, no specify the ip address",\
                                response =  "please specify the ip address.")
        
        if user_password is None:
            user_password = get_random_password()
        
        conn = self.dba_opers.get_mysql_connection()
        
        self.dba_opers.create_user(conn, userName, user_password, ip_address)
        
        if 'manager' ==  role:
            self.dba_opers.grant_manager_privileges(conn, userName, user_password, dbName, ip_address, 
                                               max_queries_per_hour, 
                                               max_updates_per_hour, 
                                               max_connections_per_hour, 
                                               max_user_connections)
        elif 'wr' == role:
            self.dba_opers.grant_wr_privileges(conn, userName, user_password, dbName, ip_address, 
                                               max_queries_per_hour, 
                                               max_updates_per_hour, 
                                               max_connections_per_hour, 
                                               max_user_connections)
            
        elif 'ro' == role:
            max_updates_per_hour = 1
            self.dba_opers.grant_readonly_privileges(conn, userName, user_password, dbName, ip_address, 
                                               max_queries_per_hour, 
                                               max_connections_per_hour, 
                                               max_user_connections)
            
        else:
            #use try catch to close the conn
            conn.close()
            
            raise HTTPAPIError(status_code=417, error_detail="when create db's user, the role type is un-valid, the error type is " + role,\
                                notification = "direct", \
                                log_message= "when create db's user, the role type is un-valid, the error type is " + role,\
                                response =  "please valid the specified role, the type is [manager,ro,wr]")
        
        self.dba_opers.flush_privileges(conn)
        
        #use try catch to close the conn
        conn.close()
        
        #check if exist cluster
        clusterUUID = self.zkOper.getClusterUUID()
        
        userProps = {'role':role,
                     'max_queries_per_hour':max_queries_per_hour,
                     'max_updates_per_hour':max_updates_per_hour,
                     'max_connections_per_hour':max_connections_per_hour,
                     'max_user_connections':max_user_connections}
        self.zkOper.write_user_info(clusterUUID,dbName,userName,ip_address,userProps)
        
        dict = {}
#        dict.setdefault("code", '000000')
        dict.setdefault("message", "user has been created successful!")
        dict.setdefault("user_role", role)
        dict.setdefault("user_name", userName)
        dict.setdefault("user_password", user_password)
        self.finish(dict)
        
    
    
    def put(self):
        dbName = self.get_argument("dbName", None)
        userName = self.get_argument("userName", None)
        ip_address = self.get_argument("ip_address", '%')
        max_queries_per_hour = self.get_argument("max_queries_per_hour", None)
        max_updates_per_hour = self.get_argument("max_updates_per_hour", None)
        max_connections_per_hour = self.get_argument("max_connections_per_hour", None)
        max_user_connections = self.get_argument("max_user_connections", None)
        
        if dbName is None:
            raise HTTPAPIError(status_code=417, error_detail="when modify db's user, no specify the database name",\
                                notification = "direct", \
                                log_message= "when modify db's user, no specify the database name",\
                                response =  "please specify the database name.")
        
        if userName is None:
            raise HTTPAPIError(status_code=417, error_detail="when modify db's user, no specify the user name",\
                                notification = "direct", \
                                log_message= "when modify db's user, no specify the user name",\
                                response =  "please specify the user name.")
        
        if ip_address is None:
            raise HTTPAPIError(status_code=417, error_detail="when modify db's user, no specify the ip address",\
                                notification = "direct", \
                                log_message= "when modify db's user, no specify the ip address",\
                                response =  "please specify the ip address.")
        
        if max_queries_per_hour is None and max_updates_per_hour is None and max_connections_per_hour is None and max_user_connections is None:
            raise HTTPAPIError(status_code=417, error_detail="when modify db's user, no specify any modified parameter",\
                                notification = "direct", \
                                log_message= "when modify db's user, no specify any modified parameter",\
                                response =  "please specify any one or all of following parameter:[max_queries_per_hour,max_updates_per_hour,max_connections_per_hour,max_user_connections]")
        
        clusterUUID = self.zkOper.getClusterUUID()
        
        user_limit_map = self.zkOper.retrieve_user_limit_props(clusterUUID, dbName, userName, ip_address)
        
        if user_limit_map == {}:
            raise HTTPAPIError(status_code=417, error_detail="when modify db's user, no found specified user!",\
                                notification = "direct", \
                                log_message = "when modify db's user, no found specified user!",\
                                response =  "please check the valid of the specified user, because the system no found the user!")
            
        if max_queries_per_hour is None:
            max_queries_per_hour = user_limit_map.get('max_queries_per_hour')
            
        if max_updates_per_hour is None:
            max_updates_per_hour = user_limit_map.get('max_updates_per_hour')
            
        if max_connections_per_hour is None:
            max_connections_per_hour = user_limit_map.get('max_connections_per_hour')
            
        if max_user_connections is None:
            max_user_connections = user_limit_map.get('max_user_connections')
        
        conn = self.dba_opers.get_mysql_connection()
        
        self.dba_opers.grant_resource_limit(conn, userName, dbName, ip_address, 
                                           max_queries_per_hour, 
                                           max_updates_per_hour, 
                                           max_connections_per_hour, 
                                           max_user_connections)
        
        self.dba_opers.flush_privileges(conn)
        
        #use try catch to close the conn
        conn.close()
        
        userProps = {'role':user_limit_map.get('role'),
                     'max_queries_per_hour':max_queries_per_hour,
                     'max_updates_per_hour':max_updates_per_hour,
                     'max_connections_per_hour':max_connections_per_hour,
                     'max_user_connections':max_user_connections}
        self.zkOper.write_user_info(clusterUUID,dbName,userName,ip_address,userProps)
        
        dict = {}
        dict.setdefault("message", "modify the user's resource limit successfully!")
        dict.setdefault("db_name", dbName)
        dict.setdefault("user_name", userName)
        self.finish(dict)
        
        
        
    def delete(self, dbName, userName, ipAddress):
        if not dbName:
            raise HTTPAPIError(status_code=417, error_detail="when remove db's user, no specify the database name",\
                                notification = "direct", \
                                log_message= "when remove db's user, no specify the database name",\
                                response =  "please specify the database name.")
        
        if not userName:
            raise HTTPAPIError(status_code=417, error_detail="when remove db's user, no specify the user name",\
                                notification = "direct", \
                                log_message= "when remove db's user, no specify the user name",\
                                response =  "please specify the user name.")
        
        if not ipAddress:
            raise HTTPAPIError(status_code=417, error_detail="when remove db's user, no specify the ip address",\
                                notification = "direct", \
                                log_message= "when remove db's user, no specify the ip address",\
                                response =  "please specify the ip address.")
        
        conn = self.dba_opers.get_mysql_connection()
        self.dba_opers.delete_user(conn, userName, ipAddress)
        #use try catch to close the conn
        conn.close()
        
        #check if exist cluster
        clusterUUID = self.zkOper.getClusterUUID()
        self.zkOper.remove_db_user(clusterUUID, dbName, userName, ipAddress)
        
        dict = {}
        dict.setdefault("message", "removed user successfully!")
        dict.setdefault("user_name", userName)
        dict.setdefault("ip_address", ipAddress)
        self.finish(dict)
        
# -*- coding: utf-8 -*-

'''
Created on 2013-7-21

@author: asus
'''
import logging

from base import APIHandler
from common.tornado_basic_auth import require_basic_auth
from common.dba_opers import DBAOpers
from common.configFileOpers import ConfigFileOpers
from common.utils import get_random_password
from common.utils.exceptions import HTTPAPIErrorException

# create manager user in mcluster
# eg. curl --user root:root -d "role=manager&dbName=managerTest&userName=zbz" "http://localhost:8888/dbUser"

# create readonly user in mcluster
# eg. curl --user root:root -d "role=ro&dbName=managerTest&userName=zbz" "http://localhost:8888/dbUser"

# create write_read user in mcluster
# eg. curl --user root:root -d "role=wr&dbName=managerTest&userName=zbz" "http://localhost:8888/dbUser"

# delete database user in mcluster
# eg. curl --user root:root -X DELETE "http://localhost:8888/dbUser/{dbName}/{userName}/{ipAddress}"

# update database user parameter
# eg.
# curl --user root:root -d "dbName=managerTest&userName=zbz&ip_address=127.0.0.1&max_queries_per_hour=200" "http://localhost:8888/dbUser"
# curl --user root:root -d "dbName=managerTest&userName=zbz&ip_address=127.0.0.1&max_updates_per_hour=200" "http://localhost:8888/dbUser"
# curl --user root:root -d "dbName=managerTest&userName=zbz&ip_address=127.0.0.1&max_connections_per_hour=200" "http://localhost:8888/dbUser"
# curl --user root:root -d "dbName=managerTest&userName=zbz&ip_address=127.0.0.1&max_user_connections=200" "http://localhost:8888/dbUser"


@require_basic_auth
class DBUser(APIHandler):

    dba_opers = DBAOpers()

    conf_opers = ConfigFileOpers()
    def post(self):
        role = self.get_argument("role", None)
        dbName = self.get_argument("dbName", None)
        userName = self.get_argument("userName", None)
        user_password = self.get_argument("user_password", None)
        ip_address = self.get_argument("ip_address", '%')
        max_queries_per_hour = self.get_argument("max_queries_per_hour", 0)
        max_updates_per_hour = self.get_argument("max_updates_per_hour", 0)
        max_connections_per_hour = self.get_argument("max_connections_per_hour", 0)
        max_user_connections = self.get_argument("max_user_connections", 200)
        dict = {}
        dict = self.request.arguments
        if dict.has_key("user_password"):
            del dict["user_password"]
        logging.info(str(dict))

        if role is None:
            raise HTTPAPIErrorException("when create db's user, no specify the user role, please specify the user role.",
                                        status_code=417)

        if dbName is None:
            raise HTTPAPIErrorException("when create db's user, no specify the database name, please specify the database name.",
                                        status_code=417)

        if userName is None:
            raise HTTPAPIErrorException("when create db's user, no specify the user name, please specify the user name.",
                                        status_code=417)

        if ip_address is None:
            raise HTTPAPIErrorException("when create db's user, no specify the ip address, please specify the ip address.",
                                        status_code=417)

        if user_password is None:
            user_password = get_random_password()

        existed_flag =  "true"

        conn = self.dba_opers.get_mysql_connection()
        try:
            existed_flag = self.dba_opers.check_if_existed_database(conn, dbName)
            if existed_flag == "false":
                raise HTTPAPIErrorException("Please create database " + dbName + " first",
                                            status_code=417)

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
                # use try catch to close the conn
                # conn.close()
                raise HTTPAPIErrorException("please valid the specified role, the type is [manager,ro,wr]", status_code=417)

            self.dba_opers.flush_privileges(conn)
        finally:
            conn.close()

        # check if exist cluster
        zkOper = self.retrieve_zkOper()
        clusterUUID = zkOper.getClusterUUID()

        userProps = {'role': role,
                     'max_queries_per_hour': max_queries_per_hour,
                     'max_updates_per_hour': max_updates_per_hour,
                     'max_connections_per_hour': max_connections_per_hour,
                     'max_user_connections': max_user_connections}
        zkOper.write_user_info(clusterUUID, dbName, userName, ip_address, userProps)

        result = {}
#        dict.setdefault("code", '000000')
        result.setdefault("message", "user has been created successful!")
        result.setdefault("user_role", role)
        result.setdefault("user_name", userName)
        result.setdefault("user_password", user_password)
        self.finish(result)

    def put(self):
        dbName = self.get_argument("dbName", None)
        userName = self.get_argument("userName", None)
        ip_address = self.get_argument("ip_address", '%')
        max_queries_per_hour = self.get_argument("max_queries_per_hour", None)
        max_updates_per_hour = self.get_argument("max_updates_per_hour", None)
        max_connections_per_hour = self.get_argument("max_connections_per_hour", None)
        max_user_connections = self.get_argument("max_user_connections", None)
        role = self.get_argument("role", None)

        if dbName is None:
            raise HTTPAPIErrorException("when modify db's user, no specify the database name, please specify the database name.",
                                        status_code=417)

        if userName is None:
            raise HTTPAPIErrorException("when modify db's user, no specify the user name, please specify the user name.",
                                        status_code=417)

        if ip_address is None:
            raise HTTPAPIErrorException("when modify db's user, no specify the ip address, please specify the ip address.",
                                        status_code=417)

        if max_queries_per_hour is None and max_updates_per_hour is None and max_connections_per_hour is None and max_user_connections is None:
            raise HTTPAPIErrorException("when modify db's user, no specify any modified parameter, please specify the ip address.\
                                         please specify any one or all of following parameter:[max_queries_per_hour,\
                                         max_updates_per_hour,max_connections_per_hour,max_user_connections]",
                                        status_code=417)

        if role is None:
            raise HTTPAPIErrorException("when modify db's user, no specify the role, please specify the role.",
                                        status_code=417)

        conn = self.dba_opers.get_mysql_connection()
        try:
            zkOper = self.retrieve_zkOper()
            clusterUUID = zkOper.getClusterUUID()
            user_limit_map = {}
            if not max_queries_per_hour or not max_updates_per_hour or not max_connections_per_hour or not max_user_connections:
                user_limit_map = zkOper.retrieve_user_limit_props(clusterUUID, dbName, userName, ip_address)
                if not user_limit_map:
                    raise HTTPAPIErrorException("when modify db's user, no found specified user!\
                                                 please check the valid of the specified user, because the system no found the user!",
                                                status_code=417)

            if max_queries_per_hour is None:
                max_queries_per_hour = user_limit_map.get('max_queries_per_hour')

            if max_updates_per_hour is None:
                max_updates_per_hour = user_limit_map.get('max_updates_per_hour')

            if max_connections_per_hour is None:
                max_connections_per_hour = user_limit_map.get('max_connections_per_hour')

            if max_user_connections is None:
                max_user_connections = user_limit_map.get('max_user_connections')

            self.dba_opers.grant_resource_limit(conn, userName, dbName, ip_address, role,
                                                max_queries_per_hour,
                                                max_updates_per_hour,
                                                max_connections_per_hour,
                                                max_user_connections)

            self.dba_opers.flush_privileges(conn)

            userProps = {'role': user_limit_map.get('role'),
                         'max_queries_per_hour': max_queries_per_hour,
                         'max_updates_per_hour': max_updates_per_hour,
                         'max_connections_per_hour': max_connections_per_hour,
                         'max_user_connections': max_user_connections}
            zkOper.write_user_info(clusterUUID, dbName, userName, ip_address, userProps)
        finally:
            conn.close()

        result = {}
        result.setdefault("message", "modify the user's resource limit successfully!")
        result.setdefault("db_name", dbName)
        result.setdefault("user_name", userName)
        self.finish(result)

    def delete(self, dbName, userName, ipAddress):
        if not dbName:
            raise HTTPAPIErrorException("when remove db's user, no specify the database name,\
                                         please specify the database name.",
                                        status_code=417)

        if not userName:
            raise HTTPAPIErrorException("when remove db's user, no specify the user name,\
                                         please specify the user name.",
                                        status_code=417)

        if not ipAddress:
            raise HTTPAPIErrorException("when remove db's user, no specify the ip address,\
                                         please specify the ip address.",
                                        status_code=417)

        conn = self.dba_opers.get_mysql_connection()

        try:
            self.dba_opers.delete_user(conn, userName, ipAddress)
        finally:
            conn.close()

        # check if exist cluster
        zkOper = self.retrieve_zkOper()
        clusterUUID = zkOper.getClusterUUID()
        zkOper.remove_db_user(clusterUUID, dbName, userName, ipAddress)

        result = {}
        result.setdefault("message", "removed user successfully!")
        result.setdefault("user_name", userName)
        result.setdefault("ip_address", ipAddress)
        self.finish(result)

#-*- coding: utf-8 -*-

'''
Created on 2013-7-21

@author: asus
'''
from base import APIHandler
from common.tornado_basic_auth import require_basic_auth
from tornado.options import options
from common.dba_opers import DBAOpers
from common.configFileOpers import ConfigFileOpers
from common.utils import get_random_password
from common.utils.exceptions import HTTPAPIError
from common.db_stat_opers import DBStatOpers
from common.node_mysql_service_opers import Node_Mysql_Service_Opers
from common.invokeCommand import InvokeCommand
from common.helper import is_monitoring, get_localhost_ip
import socket
import datetime
import time
import logging
from tornado.web import asynchronous
from tornado.gen import engine
from common.utils.asyc_utils import run_on_executor, run_callback

# create database in mcluster
# eg. curl --user root:root -d "dbName=managerTest&userName=zbz" "http://localhost:8888/db"

# delete database in mcluster
# eg. curl --user root:root -X DELETE "http://localhost:8888/db/{dbName}"

TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

@require_basic_auth
class DBOnMCluster(APIHandler):
    dba_opers = DBAOpers()
    
    conf_opers = ConfigFileOpers()
    
    def post(self):
        dbName = self.get_argument("dbName", None)
        userName = self.get_argument("userName", None)
        ip_address = self.get_argument("ip_address", '%')
        max_queries_per_hour = self.get_argument("max_queries_per_hour", 0)
        max_updates_per_hour = self.get_argument("max_updates_per_hour", 0)
        max_connections_per_hour = self.get_argument("max_connections_per_hour", 0)
        max_user_connections = self.get_argument("max_user_connections", 200)
        userPassword = get_random_password()
        conn = self.dba_opers.get_mysql_connection()
        
        try:
            self.dba_opers.craete_database(conn, dbName)
            self.dba_opers.create_user(conn, userName, userPassword, ip_address)
            self.dba_opers.grant_manager_privileges(conn, userName, userPassword, dbName, ip_address, 
                                                   max_queries_per_hour, 
                                                   max_updates_per_hour, 
                                                   max_connections_per_hour, 
                                                   max_user_connections)
            self.dba_opers.flush_privileges(conn)
        finally:
            conn.close()
        
        #check if exist cluster
        dbProps = {'db_name':dbName}
        
        zkOper = self.retrieve_zkOper()
        clusterUUID = zkOper.getClusterUUID()
        zkOper.write_db_info(clusterUUID, dbName, dbProps)
    
        userProps = {'role':'manager',
                     'max_queries_per_hour':max_queries_per_hour,
                     'max_updates_per_hour':max_updates_per_hour,
                     'max_connections_per_hour':max_connections_per_hour,
                     'max_user_connections':max_user_connections}
        zkOper.write_user_info(clusterUUID,dbName,userName,ip_address,userProps)
            
        result = {}
        result.setdefault("message", "database create successful")
        result.setdefault("manager_user_name", userName)
        result.setdefault("manager_user_password", userPassword)
        self.finish(result)
        
        
        
    def delete(self, dbName):
        if not dbName:
            raise HTTPAPIError(status_code=417, error_detail="when remove the db, no have database name",\
                                notification = "direct", \
                                log_message= "when remove the db, no have database name",\
                                response =  "please provide database name you want to removed!")
        
        zkOper = self.retrieve_zkOper()
        clusterUUID = zkOper.getClusterUUID()
        user_ipAddress_map = zkOper.retrieve_db_user_prop(clusterUUID, dbName)
    
        conn = self.dba_opers.get_mysql_connection()
    
        try:
            if user_ipAddress_map is not None:
                for (user_name,ip_address) in user_ipAddress_map.items():
                    self.dba_opers.delete_user(conn, user_name, ip_address)
    
                self.dba_opers.drop_database(conn, dbName)
        finally:
            conn.close()
    
        user_name_list = ''
        if user_ipAddress_map is not None:
            for (user_name,ip_address) in user_ipAddress_map.items():
                zkOper.remove_db_user(clusterUUID, dbName, user_name, ip_address)
                user_name_list += user_name + ","
            
        zkOper.remove_db(clusterUUID, dbName)
            
        result = {}
        result.setdefault("message", "database remove successful!")
        result.setdefault("removed_db_name", dbName)
        result.setdefault("removed_user_with_db_name", user_name_list)
        self.finish(result)

# After mcluster shut down, manager want to recover the cluster, then need to retrieve the node's uuid and seqno. 
# Though this api, user can get it.
# eg. curl "http://localhost:8888/inner/db/recover/uuid_seqno" 
class Inner_DB_Retrieve_Recover_UUID_Seqno(APIHandler):
    
    node_mysql_service_opers = Node_Mysql_Service_Opers()
    
    def get(self):
        result = self.node_mysql_service_opers.retrieve_recover_position()
        self.finish(result)

        
# when mcluster db instance's current connections exceed to 70 percent of max connections,
# we need to know this issue and notification.
# eg. curl "http://localhost:8888/inner/db/check/cur_conns" 
class Inner_DB_Check_CurConns(APIHandler):
    dba_opers = DBAOpers()
    
    def get(self):
        zkOper = self.retrieve_zkOper()
        if not is_monitoring(get_localhost_ip(), zkOper):
            self.finish("true")
            return
        conn = self.dba_opers.get_mysql_connection()
        
        if conn is None:
            self.finish("false")
            return
        
        try:
            current_connections_rows = self.dba_opers.show_processlist(conn)
            max_connections_rows = self.dba_opers.show_variables(conn,'max_connections')
        finally:
            conn.close()
        
        current_connections_count = len(current_connections_rows)
        max_connections_rows_dict = dict(max_connections_rows)
        max_connections = max_connections_rows_dict.get("max_connections")
        
        if current_connections_count/int(max_connections) < 0.7:
            self.finish("true")
            return
        
        self.finish("false")


# when mcluster db instance's current connections exceed to 80 percent of max user connections,
# we need to know this issue and notification.
# eg. curl "http://localhost:8888/inner/db/check/cur_user_conns" 
class Inner_DB_Check_User_CurConns(APIHandler):
    dba_opers = DBAOpers()
    
    def get(self):
        zkOper = self.retrieve_zkOper()
        if not is_monitoring(get_localhost_ip(), zkOper):
            self.finish("true")
            return
        
        conn = self.dba_opers.get_mysql_connection()
        if conn is None:
            self.finish("false")
            return
        
        '''
        @todo: dbs[0] need to refactor
        '''
        clusterUUID = zkOper.getClusterUUID()
        
        db_name = None
        dbs = zkOper.retrieve_db_list()
        if [] != dbs:
            db_name = dbs[0]
            
        user_prop_dict = {}
        if None is not db_name:
            user_prop_dict = zkOper.retrieve_db_user_prop(clusterUUID, db_name)
            
        try:
            for user_prop in user_prop_dict:
                max_user_connections_rows = self.dba_opers.show_user_max_conn(conn, user_prop, user_prop_dict[user_prop])
                current_user_connections_rows = self.dba_opers.show_user_current_conn(conn, user_prop, user_prop_dict[user_prop])
                if int(current_user_connections_rows) > int(max_user_connections_rows) * 0.8:
                    self.finish("false")
                    return
        finally:
            conn.close()
        
        self.finish("true")
        
   
# when mcluster db instance's wsrep status is not 'ON',
# we need to know this issue and notification.
# eg. curl "http://localhost:8888/inner/db/check/wsrep_status" 
class Inner_DB_Check_WsrepStatus(APIHandler):
    
    dba_opers = DBAOpers()
    
    def get(self):
        zkOper = self.retrieve_zkOper()
        
        if not is_monitoring(get_localhost_ip(), zkOper):
            self.finish("true")
            return
        try:
            check_result = self.dba_opers.retrieve_wsrep_status()
            logging.info("check_wsrepstatus : %s" %(check_result))
        except:
            error_message = "connection break down"
            raise HTTPAPIError(status_code=417, error_detail= error_message,\
                            notification = "direct", \
                            log_message = error_message,\
                            response =  error_message)
            
        if check_result == False:
            self.finish("false")
            return
        
        self.finish("true")
        
        
# check whether mcluster db can write and read
# eg. curl "http://localhost:8888/inner/db/check/wr"
class Inner_DB_Check_WR(APIHandler):
    
    dba_opers = DBAOpers()
    
    confOpers = ConfigFileOpers()
    
    invokeCommand = InvokeCommand()
 
#     
    def get(self):
        conn = self.dba_opers.get_mysql_connection()
        try:
            dataNodeProKeyValue = self.confOpers.getValue(options.data_node_property, ['dataNodeIp'])
            data_node_ip = dataNodeProKeyValue['dataNodeIp']
            
            zkOper = self.retrieve_zkOper()
            started_ip_list = zkOper.retrieve_started_nodes()
            identifier = socket.gethostname()
        
            '''
            @todo: review the comment code for arbitrator way
            '''
#           ret_dict = self.confOpers.getValue(options.data_node_property, ['dataNodeName','dataNodeIp'])
#           node_name = ret_dict['dataNodeName']
#           obj = re.search("-n-2", node_name)
#           if obj != None:
#               self.finish("true") 
#               return

            if conn is None:
                if data_node_ip in started_ip_list:
                    zkOper.remove_started_node(data_node_ip)
                    self.invokeCommand.run_check_shell(options.kill_innotop)
                self.finish("false")
                return
            
            zkOper.write_started_node(data_node_ip)

            if not is_monitoring(get_localhost_ip(), zkOper):
                self.finish("true")
                return
            
            dbName = 'monitor'
            n_time = datetime.datetime.now()
            
            h = n_time.hour
            min = n_time.minute
            offset = h/6

            tbName = ''
         
            prefix_tb_name = 'tem'
            mid_tb_name = str(identifier)
            mid_tb_name_rps = mid_tb_name.replace("-", "_")
            pre_tbname = prefix_tb_name + mid_tb_name_rps
            for i in range(4):
                tbName = pre_tbname + "_" + str(i)
                self.dba_opers.check_create_table(conn, tbName, dbName )
                
            tbName = pre_tbname +"_" + str(offset)
            
            del_tbName = ''
            ft = float(time.time())
            if  h % 6 == 0 and min <= 59 and (1000000*ft) % 10 == 0:
                int_tbName = (offset + 2 ) % 4
                del_tbName = "%s_%s" %(pre_tbname,int_tbName)
                self.dba_opers.delete_tb_contents(conn, del_tbName, dbName)
                logging.info('delete the contents in database (%s) before 12 hours success!' % (del_tbName))

            
            str_time = n_time.strftime(TIME_FORMAT)
            self.dba_opers.insert_record_time(conn, str_time , identifier ,tbName , dbName)
            logging.info('Insert time %s into table %s ' % (str_time, tbName))
            
            record_time = self.dba_opers.query_record_time(conn ,identifier ,tbName , dbName)
            
        except Exception,e:
            return_flag = 'false'
            logging.error(e)
            self.finish(return_flag)
            return
        finally:
            conn.close()
        
        t_threshold = options.delta_time
        n_stamp_time = time.time()
        record_stamp_time = time.mktime(time.strptime(record_time, TIME_FORMAT))
        delta_time = n_stamp_time - record_stamp_time
        
        if delta_time > t_threshold:
            error_message = 'delta_time between read and write exceed the threshold time, the delta_time is %s' % (delta_time)
            raise HTTPAPIError(status_code=500, error_detail= error_message,\
                         notification = "direct", \
                         log_message= error_message,\
                         response = error_message)
                    
        return_flag = 'true'       
        self.finish(return_flag)
        

# retrieve the database stat with innotop
# eg. curl "http://localhost:8888/db/all/stat"
class DBStat(APIHandler):
    
    stat_opers = DBStatOpers()
    
    @asynchronous
    @engine
    def get(self):
        args = self.request.arguments
        result = yield self.do(args)
        self.finish(result)
    
    @run_on_executor()
    @run_callback
    def do(self, args):
        if len(args) == 0:
            return_dict = self.stat_opers.stat()
        else:
            if args['inner'][0] == "true":
                return_dict = self.stat_opers.stat("inner")
                
        return return_dict

# retrieve the database stat with innotop
# eg. curl "http://localhost:8888/db/all/stat/rowsoper/total"        
class StatRowsOperTotal(APIHandler):
    stat_opers = DBStatOpers()
    
    @asynchronous
    @engine
    def get(self):
        result = yield self.do()
        self.finish(result)
    
    @run_on_executor()
    @run_callback
    def do(self):
        return_dict = self.stat_opers.stat_rows_oper_total()
        return return_dict   
        
# retrieve the database stat with innotop
# eg. curl "http://localhost:8888/db/all/stat/rowsoper/ps"        
class StatRowsOperPS(APIHandler):
    stat_opers = DBStatOpers()
    
    @asynchronous
    @engine
    def get(self):
        result = yield self.do()
        self.finish(result)
        
    @run_on_executor()
    @run_callback
    def do(self):
        return_dict = self.stat_opers.stat_rows_oper_per_second()
        return return_dict  
        
        
# retrieve the database stat with innotop
# eg. curl "http://localhost:8888/db/all/stat/innobuffer/memallco"        
class StatInnoBufferMemAlloc(APIHandler):
    stat_opers = DBStatOpers()
    
    @asynchronous
    @engine
    def get(self):
        result = yield self.do()
        self.finish(result)
        
    @run_on_executor()
    @run_callback
    def do(self):
        return_dict = self.stat_opers.stat_innodb_buffer_mem_alloc()
        return return_dict  
        
# retrieve the database stat with innotop
# eg. curl "http://localhost:8888/db/all/stat/innobuffer/page"        
class StatInnoBufferPage(APIHandler):
    stat_opers = DBStatOpers()
    
    @asynchronous
    @engine
    def get(self):
        result = yield self.do()
        self.finish(result)
    
    @run_on_executor()
    @run_callback
    def do(self):
        return_dict = self.stat_opers.stat_innodb_buffer_page()
        return return_dict  
        
# retrieve the database stat with innotop
# eg. curl "http://localhost:8888/db/all/stat/innobuffer/pool"        
class StatInnoBufferPool(APIHandler):
    stat_opers = DBStatOpers()
    
    @asynchronous
    @engine
    def get(self):
        result = yield self.do()
        self.finish(result)
        
    @run_on_executor()
    @run_callback
    def do(self):
        return_dict = self.stat_opers.stat_innodb_buffer_buffer_pool()
        return return_dict  
        
        
# retrieve the database stat with innotop
# eg. curl "http://localhost:8888/db/all/stat/variablestatus/ps"        
class StatVariableStatusPS(APIHandler):
    stat_opers = DBStatOpers()
    
    @asynchronous
    @engine
    def get(self):
        result = yield self.do()
        self.finish(result)
        
    @run_on_executor()
    @run_callback
    def do(self):
        return_dict = self.stat_opers.stat_variable_status_ps()
        return return_dict  
        
        
# retrieve the database stat with innotop
# eg. curl "http://localhost:8888/db/all/stat/variablestatus/used"        
class StatVariableStatusUsed(APIHandler):
    stat_opers = DBStatOpers()
    
    @asynchronous
    @engine
    def get(self):
        result = yield self.do()
        self.finish(result)
        
    @run_on_executor()
    @run_callback
    def do(self):
        return_dict = self.stat_opers.stat_variable_status_used()
        return return_dict  
        
        
# retrieve the database stat with innotop
# eg. curl "http://localhost:8888/db/all/stat/variablestatus/ration"        
class StatVariableStatusRation(APIHandler):
    stat_opers = DBStatOpers()
    
    @asynchronous
    @engine
    def get(self):
        result = yield self.do()
        self.finish(result)
        
    @run_on_executor()
    @run_callback
    def do(self):
        return_dict = self.stat_opers.stat_variable_status_ration()
        return return_dict  
        
# retrieve the wsrep status with show status way
# eg. curl "http://localhost:8888/db/all/stat/wsrepstatus/flow_control_paused"        
class StatWsrepStatusFlowControlPaused(APIHandler):
    stat_opers = DBStatOpers()
    
    @asynchronous
    @engine
    def get(self):
        result = yield self.do()
        self.finish(result)
        
    @run_on_executor()
    @run_callback
    def do(self):
        return_dict = self.stat_opers.stat_wsrep_status_flow_control_paused()
        return return_dict  
        
        
# retrieve the wsrep status with show status way
# eg. curl "http://localhost:8888/db/all/stat/wsrepstatus/slowest_node_param"        
class StatWsrepStatusSlowestNodeParam(APIHandler):
    stat_opers = DBStatOpers()
    
    @asynchronous
    @engine
    def get(self):
        result = yield self.do()
        self.finish(result)
        
    @run_on_executor()
    @run_callback
    def do(self):
        return_dict = self.stat_opers.stat_wsrep_status_slowest_node_param()
        return return_dict  
        
        
# retrieve the wsrep status with show status way
# eg. curl "http://localhost:8888/db/all/stat/wsrepstatus/slowest_network_param"        
class StatWsrepStatusSlowestNetworkParam(APIHandler):
    stat_opers = DBStatOpers()
    
    @asynchronous
    @engine
    def get(self):
        result = yield self.do()
        self.finish(result)
        
    @run_on_executor()
    @run_callback
    def do(self):
        return_dict = self.stat_opers.stat_wsrep_status_slowest_network_param()
        return return_dict  
        
# retrieve the binlog logendlogpos of mcluster
# eg. curl "http://localhost:8888/db/binlog/pos?xid=16754"
class BinlogPos(APIHandler):
    stat_opers = DBStatOpers()
    @asynchronous
    @engine
    def post(self):
        params = self.get_all_arguments()
        return_result = yield self.stat_opers.stat_binlog_eng_log_pos(params)  
        self.finish(return_result)

#retrieve opened binlog node list of mcluster
class BinLogNodestat(APIHandler):
    stat_opers = DBStatOpers()
    @asynchronous
    @engine
    def post(self):
        #params = self.get_all_arguments()
        return_result = yield self.stat_opers.bin_log_node_stat()  
        self.finish(return_result)
        
# retrieve the detailed status of mcluster
# eg. curl "http://localhost:8888/node/stat/info?stat_connection_count_command=mysql"
class StatMysqlInfo(APIHandler):
    
    dba_opers = DBAOpers()
    
    @asynchronous
    @engine
    def post(self):
        params = self.get_all_arguments()
        result = yield self.dba_opers.retrieve_node_info_stat(params)
        self.finish(result)


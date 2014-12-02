from common.invokeCommand import InvokeCommand
from common.helper import retrieve_kv_from_db_rows
from common.dba_opers import DBAOpers
from common.zkOpers import ZkOpers
from tornado.gen import Callback, Wait
from tornado.options import options
from abc import abstractmethod
from common.helper import check_leader

import logging
import tornado.httpclient
import datetime
import re

TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

class Check_Status_Base(object):
    
    zkOper = ZkOpers('127.0.0.1',2181)
    
    def __init__(self):
        if self.__class__ == Check_Status_Base:
            raise NotImplementedError, \
            "Cannot create object of class Check_Status_Base"
    
    @abstractmethod
    def check(self, data_node_info_list):
        raise NotImplementedError, "Cannot call abstract method"
    
    @abstractmethod
    def retrieve_alarm_level(self, total_count, success_count, failed_count):
        raise NotImplementedError, "Cannot call abstract method"
    
    @tornado.gen.engine
    def check_status(self, data_node_info_list, url_post, monitor_type, monitor_key):
        #access to the target ip machine to retrieve the dict,then modify the config
        http_client = tornado.httpclient.AsyncHTTPClient()
        leader_flag = check_leader()
        if leader_flag == False:
            return
            
        pre_stat = self.zkOper.retrieveClusterStatus()
        if pre_stat.has_key('_status') and pre_stat['_status'] != 'initializing':
            node_num = len(data_node_info_list)
            online_node_list = self.zkOper.retrieve_started_nodes()
            dict = {}
        
            online_num = len(online_node_list)
            if node_num == online_num:
                dict['_status'] = 'running'   
            elif node_num / 2 + 1 <= online_num < node_num:
                dict['_status'] = 'sub-health'
            else :
                dict['_status'] = 'failed' 
            logging.info(pre_stat['_status'] + " change to " + dict['_status'])
            self.zkOper.writeClusterStatus(dict) 
            
        success_count = 0
        failed_count = 0
        
        key_sets = set()
        zk_data_node_count = len(data_node_info_list)
        for i in range(zk_data_node_count):
            zk_incoming_address = data_node_info_list[i]
            requesturi = "http://"+zk_incoming_address+":"+str(options.port)+ url_post
            callback_key = "%s_%s_%s" % (monitor_type,monitor_key,zk_incoming_address)
            key_sets.add(callback_key)
            http_client.fetch(requesturi, callback=(yield Callback(callback_key)))
       
        error_record_dict = {}    
        error_record_msg = ''
        error_record_ip_list = [] 

        for i in range(len(key_sets)):
            callback_key = key_sets.pop()
            response = yield Wait(callback_key)
            if response.error:
                return_result = False
                message = "remote access,the key:%s,error message:%s" % (callback_key,response.error)
                error_record_msg += message + "|"
                logging.error(message)
            else:
                return_result = response.body.strip()
#                logging.info(return_result)
            
            if cmp('true',return_result) == 0:
                success_count += 1
            else:
                callback_key_ip = callback_key.split("_")[-1]
                error_record_ip_list.append(callback_key_ip)
                failed_count += 1

        if (error_record_msg != '' or error_record_ip_list != []):
            error_record_dict.setdefault("msg",error_record_msg)
            error_record_dict.setdefault("ip", error_record_ip_list)
     
        http_client.close()
        
        alarm_level = self.retrieve_alarm_level(zk_data_node_count, success_count, failed_count)
        
        self.write_status(zk_data_node_count, success_count, failed_count, alarm_level, error_record_dict, monitor_type, monitor_key)
        
    def write_status(self, total_count, success_count, failed_count, alarm_level, error_record_dict, monitor_type, monitor_key):
        result_dict = {}
        format_str = "total=%s, success count=%s, failed count=%s"
        format_values = (total_count, success_count, failed_count)
        message = format_str % format_values
        dt = datetime.datetime.now()
        result_dict.setdefault("message", message)
        result_dict.setdefault("alarm", alarm_level)
        result_dict.setdefault("error_record", error_record_dict)
        result_dict.setdefault("ctime", dt.strftime(TIME_FORMAT))
        
#        logging.info("monitor_type:" + monitor_type + " monitor_key:" + 
#                     monitor_key + " monitor_value:" + str(result_dict))
        
        self.zkOper.write_monitor_status(monitor_type, monitor_key, result_dict)
        

class Check_Cluster_Available(Check_Status_Base):
    
    invokeCommand = InvokeCommand()
    
    def check(self, data_node_info_list):
        shell_result = self.invokeCommand.run_check_shell(options.check_mcluster_health)
        
        message = "no avaliable data node on VIP"
        
        if shell_result:
            message = "ok"
            
        failed_count = 0
        if shell_result == None or shell_result == False:
            failed_count = 3
            
        alarm_result = self.retrieve_alarm_level(0,0,failed_count)
            
        cluster_available_dict = {}
        cluster_available_dict.setdefault("message",message)
        cluster_available_dict.setdefault("alarm",alarm_result)
        
        return cluster_available_dict
    
    def retrieve_alarm_level(self, total_count, success_count, failed_count):
        if failed_count == 3:
            return options.alarm_serious
        
        return options.alarm_nothing
    
class Check_Node_Size(Check_Status_Base):
    
    dba_opers = DBAOpers()
    
    def check(self, data_node_info_list):
        conn = self.dba_opers.get_mysql_connection()
            
        if conn == None:
            exception_dict = {}
            exception_dict.setdefault("message", "no way to connect to db")
            exception_dict.setdefault("alarm", options.alarm_serious)
            return exception_dict
        
        try:
            rows = self.dba_opers.show_status(conn)
        finally:
            conn.close()
        
        key_value = retrieve_kv_from_db_rows(rows,['wsrep_incoming_addresses','wsrep_cluster_size'])
        node_size_dict = self._check_wsrep_incoming_addresses(key_value, data_node_info_list)
        return node_size_dict
        
    def retrieve_alarm_level(self, total_count, success_count, failed_count):
        if failed_count == 0:
            return options.alarm_nothing
        elif failed_count == 1:
            return options.alarm_general
        else:
            return options.alarm_serious
        
    def _check_wsrep_incoming_addresses(self, key_value, data_node_info_list):
        if key_value == {}:
            return False
        
        lost_ip_list = [] 
        wsrep_incoming_addresses_value = key_value.get('wsrep_incoming_addresses')
        wsrep_cluster_size = key_value.get('wsrep_cluster_size')
        logging.info("[compare Mcluster the count of data node] incoming address(show status):" + 
                     wsrep_incoming_addresses_value)
        wsrep_incoming_addresses_list = wsrep_incoming_addresses_value.split(',')
        
        address_count = 0
        zk_data_node_count = len(data_node_info_list)
        for i in range(zk_data_node_count):
            zk_incoming_address_port = data_node_info_list[i] + ':3306'
            logging.info("[compare Mcluster the count of data node] address list(zk store):" + 
                         zk_incoming_address_port)
            
            if zk_incoming_address_port in wsrep_incoming_addresses_list:
                address_count = address_count + 1
            else:
                 lost_ip_list.append(data_node_info_list[i]) 

        total = zk_data_node_count
        exist = address_count
        lost = zk_data_node_count - address_count
                
        alarm_level = self.retrieve_alarm_level(total, exist, lost)
                
        node_size_dict = {}
        lost_count = zk_data_node_count - address_count
        format_str = "total=%s, exist=%s, lost=%s"
        format_values = (zk_data_node_count, address_count, lost_count)
        message = format_str % format_values
        node_size_dict.setdefault("lost_ip", lost_ip_list)
        node_size_dict.setdefault("message", message)
        node_size_dict.setdefault("alarm", alarm_level)
        
        return node_size_dict
    
class Check_DB_Anti_Item(Check_Status_Base):
    
    dba_opers = DBAOpers()
    
    def check(self, data_node_info_list):
        
        leader_flag = check_leader()
        if leader_flag == False:
            return
        conn = self.dba_opers.get_mysql_connection()

        monitor_type = "db"
        monitor_key = "existed_db_anti_item"
        error_record = ""
        
        anti_item_count = 0
        failed_count = 0
        Path_Value = {}
        Path_Value = self.zkOper.retrieve_monitor_status_value(monitor_type, monitor_key)
        if Path_Value != {}:
#             str_msg = Path_Value['message']
#             r_list = str_msg[::-1].split('=')
#             r_value = r_list[0]
#             value = int(r_value[::-1])
            failed_count = int(re.findall(r'failed count=(\d)', Path_Value['message'])[0])
        if conn == None:
            failed_count += 1
            if failed_count > 4:
                anti_item_count = 500
                error_record ="no way to connect to db"
        else:
            try:
                failed_count = 0
                anti_item_count = self.dba_opers.check_existed_myisam_table(conn)
                anti_item_count += self.dba_opers.check_existed_nopk(conn)
                anti_item_count += self.dba_opers.check_existed_fulltext_and_spatial(conn)
            finally:
                conn.close()
        
            if anti_item_count > 0:
                error_record = "mcluster existed on Myisam,Nopk,FullText,SPATIAL,please check which db right now."
                logging.info(error_record)
    
       
        alarm_level = self.retrieve_alarm_level(anti_item_count, 0, 0)
        super(Check_DB_Anti_Item, self).write_status(anti_item_count, 0, \
                                                    failed_count, \
                                                    alarm_level, error_record, monitor_type, \
                                                    monitor_key)
        
    def retrieve_alarm_level(self, total_count, success_count, failed_count):
        if total_count == 0:
            return options.alarm_nothing
        
        return options.alarm_serious
        
    
class Check_DB_WR_Avalialbe(Check_Status_Base):
    
    def __init__(self):
        super(Check_DB_WR_Avalialbe, self).__init__()
        
    @tornado.gen.engine
    def check(self, data_node_info_list):
        url_post = "/inner/db/check/wr"
        monitor_type = "db"
        monitor_key = "write_read_avaliable"
        super(Check_DB_WR_Avalialbe, self).check_status(data_node_info_list, url_post, monitor_type, monitor_key)
        
    def retrieve_alarm_level(self, total_count, success_count, failed_count):
#         message = "processing method: Check_DB_WR_Avalialbe,the total count:%s,the succes count:%s,the failed count:%s"
#         logging.info(message%(total_count, success_count, failed_count))
        if failed_count == 0:
            return options.alarm_nothing
        elif failed_count == 1:
            return options.alarm_general
        else:
            return options.alarm_serious
        
        
class Check_DB_Wsrep_Status(Check_Status_Base):
    
    def __init__(self):
        super(Check_DB_Wsrep_Status, self).__init__()
        
    @tornado.gen.engine
    def check(self, data_node_info_list):
        url_post = "/inner/db/check/wsrep_status"
        monitor_type = "db"
        monitor_key = "wsrep_status"
        super(Check_DB_Wsrep_Status, self).check_status(data_node_info_list, url_post, monitor_type, monitor_key)
        
    def retrieve_alarm_level(self, total_count, success_count, failed_count):
#         message = "processing method: Check_DB_Wsrep_Status,the total count:%s,the succes count:%s,the failed count:%s"
#         logging.info(message%(total_count, success_count, failed_count))
        if failed_count == 0:
            return options.alarm_nothing
        elif failed_count == 1:
            return options.alarm_general
        else:
            return options.alarm_serious
        
        
class Check_DB_Cur_Conns(Check_Status_Base):
    
    def __init__(self):
        super(Check_DB_Cur_Conns, self).__init__()
    
    @tornado.gen.engine
    def check(self, data_node_info_list):
        url_post = "/inner/db/check/cur_conns"
        monitor_type = "db"
        monitor_key = "cur_conns"
        super(Check_DB_Cur_Conns, self).check_status(data_node_info_list, url_post, monitor_type, monitor_key)
        
    def retrieve_alarm_level(self, total_count, success_count, failed_count):
#         message = "processing method: Check_DB_Cur_Conns,the total count:%s,the succes count:%s,the failed count:%s"
#         logging.info(message%(total_count, success_count, failed_count))
        if failed_count == 0:
            return options.alarm_nothing
        elif failed_count == 1:
            return options.alarm_general
        else:
            return options.alarm_serious
        
class Check_Node_Active(Check_Status_Base):
    
    def __init__(self):
        super(Check_Node_Active, self).__init__()
    
    @tornado.gen.engine
    def check(self, data_node_info_list):
        started_nodes_list = self.zkOper.retrieve_started_nodes()
        
        error_record = ''
        for data_node_ip in started_nodes_list:
            error_record += data_node_ip + ";"
            
        total_count = len(data_node_info_list)
        success_count = len(started_nodes_list)
        failed_count = total_count - success_count
        monitor_type = "node"
        monitor_key = "started"
        alarm_level = self.retrieve_alarm_level(total_count, success_count, failed_count)
        
        super(Check_Node_Active, self).write_status(total_count, success_count, \
                                                    failed_count, \
                                                    alarm_level, error_record, monitor_type, \
                                                    monitor_key)
        
    def retrieve_alarm_level(self, total_count, success_count, failed_count):
        if failed_count == 0:
            return options.alarm_nothing
        elif failed_count == 1:
            return options.alarm_general
        else:
            return options.alarm_serious
        
class Check_Node_Log_Health(Check_Status_Base):
    
    def __init__(self):
        super(Check_Node_Log_Health, self).__init__()
      
    @tornado.gen.engine  
    def check(self, data_node_info_list):
        url_post = "/inner/node/check/log/health"
        monitor_type = "node"
        monitor_key = "log_health"
        super(Check_Node_Log_Health, self).check_status(data_node_info_list, url_post, monitor_type, monitor_key)
        
    def retrieve_alarm_level(self, total_count, success_count, failed_count):
#         message = "processing method: Check_Node_Log_Health,the total count:%s,the succes count:%s,the failed count:%s"
#         logging.info(message%(total_count, success_count, failed_count))
        if failed_count == 0:
            return options.alarm_nothing
        elif failed_count == 1:
            return options.alarm_general
        else:
            return options.alarm_serious
        
        
class Check_Node_Log_Error(Check_Status_Base):
    
    def __init__(self):
        super(Check_Node_Log_Error, self).__init__()
    
    @tornado.gen.engine
    def check(self, data_node_info_list):
        url_post = "/inner/node/check/log/error"
        monitor_type = "node"
        monitor_key = "log_error"
        super(Check_Node_Log_Error, self).check_status(data_node_info_list, url_post, monitor_type, monitor_key)
        
    def retrieve_alarm_level(self, total_count, success_count, failed_count):
#         message = "processing method: Check_Node_Log_Error,the total count:%s,the succes count:%s,the failed count:%s"
#         logging.info(message%(total_count, success_count, failed_count))
        if failed_count == 0:
            return options.alarm_nothing
        elif failed_count == 1:
            return options.alarm_general
        else:
            return options.alarm_serious
        
        
class Check_Node_Log_Warning(Check_Status_Base):
    
    def __init__(self):
        super(Check_Node_Log_Warning, self).__init__()
    
    @tornado.gen.engine    
    def check(self, data_node_info_list):
        url_post = "/inner/node/check/log/warning"
        monitor_type = "node"
        monitor_key = "log_warning"
        super(Check_Node_Log_Warning, self).check_status(data_node_info_list, url_post, monitor_type, monitor_key)
        
    def retrieve_alarm_level(self, total_count, success_count, failed_count):
#         message = "processing method: Check_Node_Log_Warning,the total count:%s,the succes count:%s,the failed count:%s"
#         logging.info(message%(total_count, success_count, failed_count))
        if failed_count == 0:
            return options.alarm_nothing
        elif failed_count == 1:
            return options.alarm_general
        else:
            return options.alarm_serious

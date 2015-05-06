from common.invokeCommand import InvokeCommand
from common.helper import retrieve_kv_from_db_rows 
from common.dba_opers import DBAOpers
from common.zkOpers import ZkOpers
from tornado.gen import Callback, Wait
from tornado.options import options
from abc import abstractmethod
from common.helper import check_leader, is_monitoring, get_localhost_ip
from common.utils.mail import send_email

import logging
import tornado.httpclient
import datetime
import re


TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

class Check_Status_Base(object):
    
    zkOper = ZkOpers()
    
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
        zk_data_node_count = len(data_node_info_list)
        pre_stat = self.zkOper.retrieveClusterStatus()
        ''' The following logic expression means 
            1. if we don't have the cluster_status node in zookeeper we will get pre_stat as {}, we will create the path in the following process.
            2. else the pre_stat is not {}, then it must have value in pre_stat dictionary and judge whether it is right or not.
        '''
        if pre_stat.has_key('_status') and pre_stat['_status'] != 'initializing' or pre_stat == {}:
            online_node_list = self.zkOper.retrieve_started_nodes()
            result = {}
        
            online_num = len(online_node_list)
            if zk_data_node_count == online_num:
                result['_status'] = 'running'   
            elif zk_data_node_count / 2 + 1 <= online_num < zk_data_node_count:
                result['_status'] = 'sub-health'
            else :
                result['_status'] = 'failed' 
            self.zkOper.writeClusterStatus(result)
            
            
        success_count = 0
        failed_count = 0
        
        http_client = tornado.httpclient.AsyncHTTPClient()
        key_sets = set()
        
        try:
            for data_node_address in data_node_info_list:
                requesturi = "http://%s:%s%s" % (data_node_address, str(options.port), url_post)
                callback_key = "%s_%s_%s" % (monitor_type,monitor_key,data_node_address)
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
                else:
                    return_result = response.body.strip()
                
                if cmp('true',return_result) == 0:
                    success_count += 1
                else:
                    callback_key_ip = callback_key.split("_")[-1]
                    error_record_ip_list.append(callback_key_ip)
                    failed_count += 1
        finally:
            http_client.close()
        
           
        if (error_record_msg != '' or error_record_ip_list != []):
            error_record_dict.setdefault("msg",error_record_msg)
            error_record_dict.setdefault("ip", error_record_ip_list)
        
        
        alarm_level = self.retrieve_alarm_level(zk_data_node_count, success_count, failed_count)
        if monitor_key == "backup":
            if failed_count >= 1:
                error_record_dict['msg']= "expired"
            else:
                error_record_dict['msg'] = "expected"
        
        self.write_status(zk_data_node_count, success_count, failed_count, alarm_level, error_record_dict, monitor_type, monitor_key)


    def write_status(self, total_count, success_count, failed_count, alarm_level, error_record_dict, monitor_type, monitor_key):
        dt = datetime.datetime.now()
        _include_timeout_num_from_response = 0
        if {} != error_record_dict:
            _error_record_message = error_record_dict.get('msg')
            _include_timeout_list = re.findall(r'HTTP 599: Timeout', str(_error_record_message))
            _include_timeout_num_from_response = len(_include_timeout_list)
        
        _timeout_num_from_zk = 0    
        if _include_timeout_num_from_response > 0:
            _monitor_value_dict = self.zkOper.retrieve_monitor_status_value(monitor_type, monitor_key)
            _timeout_num = _monitor_value_dict.get("timeout_num")
            if _timeout_num is not None:
                _timeout_num_from_zk = _timeout_num
                
            _timeout_num_from_zk += 1
                
        if _timeout_num_from_zk < 2 and _include_timeout_num_from_response > 0:
            success_count = total_count
            failed_count = 0
            alarm_level = "nothing"
            error_record_dict = {}
    
        result_dict = {
            "message": "total=%s, success count=%s, failed count=%s" % (total_count, success_count, failed_count),
            "alarm": alarm_level,
            "error_record": error_record_dict,
            "ctime": dt.strftime(TIME_FORMAT),
            "timeout_num": _timeout_num_from_zk
        }
        
        self.zkOper.write_monitor_status(monitor_type, monitor_key, result_dict)

class Check_Cluster_Available(Check_Status_Base):
    
    invokeCommand = InvokeCommand()
    
    def check(self, data_node_info_list):
        shell_result = self.invokeCommand.run_check_shell(options.check_mcluster_health)
        
        message = "no avaliable data node on VIP"
        
        if shell_result:
            message = "ok"
            
        failed_count = 0
        if shell_result == None or shell_result == False or shell_result == "false":
            failed_count = 3
        else:
            message = "ok"
            
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
        
        if not is_monitoring(get_localhost_ip(), self.zkOper):
            return
        
        conn = self.dba_opers.get_mysql_connection()
       
        monitor_type = "db"
        monitor_key = "existed_db_anti_item"
        error_record = {}
        
        anti_item_count = 0
        failed_count = 0
        _path_value = {}
        
        _path_value = self.zkOper.retrieve_monitor_status_value(monitor_type, monitor_key)
        
        if _path_value != {}:
            failed_count = int(re.findall(r'failed count=(\d)', _path_value['message'])[0])
            
        if conn == None:
            failed_count += 1
            if failed_count > 4:
                anti_item_count = 500
                error_record.setdefault("msg", "no way to connect to db")
        else:
            try:
                failed_count = 0
                anti_item_count = 0
                msg = ""
                anti_item_myisam_count = self.dba_opers.check_existed_myisam_table(conn)
                anti_item_procedure_count = self.dba_opers.check_existed_stored_procedure(conn)
                anti_item_trigger_count = self.dba_opers.check_triggers(conn)
                anti_item_nopk_count = self.dba_opers.check_existed_nopk(conn)
                anti_item_fulltext_and_spatial_count = self.dba_opers.check_existed_fulltext_and_spatial(conn)
                
                if anti_item_myisam_count :
                    anti_item_count += anti_item_myisam_count
                    msg += " Myisam,"
                    
                if anti_item_procedure_count:
                    anti_item_message = "check db status, existed stored procedure. Item's count:%s"%(str(anti_item_procedure_count))
                    self._send_monitor_email(anti_item_message)
                    
                if anti_item_trigger_count :
                    anti_item_count += anti_item_trigger_count
                    msg += " Trigger,"
                    
                if anti_item_nopk_count :
                    anti_item_count += anti_item_nopk_count
                    msg += " NOPK,"
                    
                if anti_item_fulltext_and_spatial_count:
                    anti_item_count += anti_item_fulltext_and_spatial_count
                    msg += " FullText, SPATIAL,"
            finally:
                conn.close()
        
            if anti_item_count > 0:
                error_record.setdefault("msg", "mcluster existed on %s please check which db right now." % (msg) )
                logging.info(error_record)
    
       
        alarm_level = self.retrieve_alarm_level(anti_item_count, 0, 0)
        logging.info("existed anti_item alarm_level :%s" %(alarm_level))
        super(Check_DB_Anti_Item, self).write_status(anti_item_count, 0, \
                                                    failed_count, \
                                                    alarm_level, error_record, monitor_type, \
                                                    monitor_key)
        
        
    def retrieve_alarm_level(self, total_count, success_count, failed_count):
        if total_count == 0:
            return options.alarm_nothing
        
        return options.alarm_serious
    
    def _send_monitor_email(self, anti_item_content):
        local_ip = get_localhost_ip()
        # send email
        subject = "[%s] Auti-Item existed in MySQL according to Galera way" % options.sitename
        body = self.render_string("errors/500_email.html",
                                  exception=anti_item_content)
        
        body += "\nip:" + local_ip
        
        if options.send_email_switch:
            send_email(options.admins, subject, body)
        
    
class Check_DB_WR_Avalialbe(Check_Status_Base):
    
    def __init__(self):
        super(Check_DB_WR_Avalialbe, self).__init__()
        
    @tornado.gen.engine
    def check(self, data_node_info_list):
        url_post = "/inner/db/check/wr"
        monitor_type = "db"
        monitor_key = "write_read_avaliable"
        logging.info("check_db_wr_avaliable data_node_info_list: %s ." %(data_node_info_list))
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
        
        error_record = {}
        ip = []
        for data_node_ip in started_nodes_list:
            ip.append(data_node_ip)
            
        error_record.setdefault("online_ip", ip)
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
        return options.alarm_nothing
        
#eq curl  "http://localhost:8888/backup/inner/check" backup data by full dose.
class Check_Backup_Status(Check_Status_Base):
    
    def __init__(self):
        super(Check_Backup_Status, self).__init__()
        
    @tornado.gen.engine  
    def check(self, data_node_info_list):
        url_post = "/backup/inner/check"
        monitor_type = "db"
        monitor_key = "backup"
        super(Check_Backup_Status, self).check_status(data_node_info_list, url_post, monitor_type, monitor_key)
        
    def retrieve_alarm_level(self, total_count, success_count, failed_count):
        if failed_count == 0:
            return options.alarm_nothing
        elif failed_count == 1:
            return options.alarm_general
        else:
            return options.alarm_serious

class Check_Database_User(Check_Status_Base):
    dba_opers = DBAOpers()
   
    def __init__(self):
        super(Check_Database_User, self).__init__()

    @tornado.gen.engine
    def check(self, data_node_info_list):
        #url_post = "/dbuser/inner/check"
        if not is_monitoring(get_localhost_ip(), self.zkOper):
            return
        
        monitor_type = "db"
        monitor_key = "dbuser"
        
        conn = self.dba_opers.get_mysql_connection()
        user_tuple = self.dba_opers.get_db_users(conn)
        
        user_mysql_src_dict = {}

# We convert origin tuple grabbed from mysql into list, then  combine the elements subscripted 0 ,1 as key of dict and combine the elements subscripted -3, -4 ,-5, -6 as the value of the dict.Finally we append the dict into list.
        
        for t in user_tuple:
            inner_value_list =  []
            dict_key_str = (list(t)[1] + "|" + list(t)[0])
            inner_value_list.append(list(t)[-3])
            inner_value_list.append(list(t)[-4])
            inner_value_list.append(list(t)[-5])
            inner_value_list.append(list(t)[-6])
            user_mysql_src_dict.setdefault(dict_key_str, inner_value_list)

          
        db_list = self.zkOper.retrieve_db_list()
    
        user_zk_src_list = []
        for db_name in db_list:
            db_user_list = self.zkOper.retrieve_db_user_list(db_name)
            logging.info("dbName: " + db_name + " db_user_list : " + str(db_user_list))
            for db_user in db_user_list:
                inner_list = []
                inner_list.append(db_user)
                prop = self.zkOper.get_db_user_prop(db_name, db_user)
                inner_list.append(prop)
                user_zk_src_list.append(inner_list)
        
        differ_dict_set = {}
        count_dict_set = {}
        error_record = {}
        if len(user_zk_src_list) == 0 and len(user_mysql_src_dict) == 0:
            count_dict_set.setdefault("total", 0)
            count_dict_set.setdefault("failed", 0)
            count_dict_set.setdefault("success", 0) 
            error_record.setdefault("msg", "no database users in zk neither in mysql")
            differ_dict_set.setdefault("Empty","" )

            alarm_level = self.retrieve_alarm_level(count_dict_set["total"], count_dict_set["success"], count_dict_set["failed"])
            
        else: 
            count_dict_set.setdefault("total" , 0)
            count_dict_set.setdefault("failed" , 0)
            count_dict_set.setdefault("success" , 0)
         
            self.compare_center(user_mysql_src_dict, user_zk_src_list, differ_dict_set ,count_dict_set)
            count_dict_set["total"] = count_dict_set["success"]  + count_dict_set["failed"]
            alarm_level = self.retrieve_alarm_level(count_dict_set["total"], count_dict_set["success"], count_dict_set["failed"])
        
        total_count = count_dict_set["total"]
        failed_count = count_dict_set["failed"]
        success_count = count_dict_set["success"]
        if differ_dict_set:
            error_record.setdefault("dif", differ_dict_set)
       
        #logging.info("Check_db_user -----> error_record" + str(error_record))
        #logging.info("Check_db_user <------> " + str(count_dict_set))
        super(Check_Database_User, self).write_status(total_count, success_count, \
                                                    failed_count, \
                                                    alarm_level, error_record, monitor_type, \
                                                    monitor_key)

    def compare_center(self, _user_mysql_src_dict, _user_zk_src_list, _differ_dict_set ,_count_dict):
        _user_mysql_src_dict_keys = _user_mysql_src_dict.keys()
        logging.info("_user_mysql_src_dict_keys" + str(_user_mysql_src_dict_keys))
        logging.info("_user_mysql_src_dict ::::" + str(_user_mysql_src_dict))
        logging.info("_user_zk_src_list: " + str(_user_zk_src_list))
        for list_iter in _user_zk_src_list:
            if list_iter[0] in _user_mysql_src_dict_keys:
                if long(list_iter[1]["max_user_connections"]) == _user_mysql_src_dict[list_iter[0]][0] and  \
                       long(list_iter[1]["max_connections_per_hour"])==  _user_mysql_src_dict[list_iter[0]][1] and \
                           long(list_iter[1]["max_updates_per_hour"]) ==  _user_mysql_src_dict[list_iter[0]][2] and \
                               long(list_iter[1]["max_queries_per_hour"]) == _user_mysql_src_dict[list_iter[0]][3] :
                    _count_dict["success"] = _count_dict["success"] + 1
                    continue
                else:
                    inner_dict = {}
                    inner_dict.setdefault("message", "different")
                    logging.info("list_iter[0] :" + str(list_iter[0]))
                    _differ_dict_set.setdefault(list_iter[0], inner_dict)
                    _count_dict["failed"] = _count_dict["failed"] + 1
            else: 
                inner_dict = {}
                inner_dict.setdefault("message", "unknown")
                logging.info("list_iter[0] :" + str(list_iter[0]))
                _differ_dict_set.setdefault(list_iter[0], inner_dict)
                _count_dict["failed"] = _count_dict["failed"] + 1
        
        _user_zk_src_keys_list  = []
        for i in range(len(_user_zk_src_list)):
            _user_zk_src_keys_list.append(_user_zk_src_list[i][0])
        logging.info("_user_zk_src_keys_list :" + str(_user_zk_src_keys_list))    
        for _user_mysql_list_iter in _user_mysql_src_dict_keys:
            if _user_mysql_list_iter not in _user_zk_src_keys_list:
                inner_dict = {}
                inner_dict.setdefault("message" , "lost")
                _differ_dict_set.setdefault(_user_mysql_list_iter, inner_dict)
                _count_dict["failed"] = _count_dict["failed"] + 1
         
                  
    def retrieve_alarm_level(self, total_count, success_count, failed_count):
        if failed_count == 0:
            return options.alarm_nothing
        else:
            return options.alarm_general
 
           
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
        return options.alarm_nothing


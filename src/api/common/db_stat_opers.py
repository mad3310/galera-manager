'''
Created on 2013-7-21

@author: asus
'''
import json
import logging
import tornado.httpclient
from common.dba_opers import DBAOpers
from tornado.options import options
from common.configFileOpers import ConfigFileOpers
from common.abstract_stat_service import Abstract_Stat_Service
from common.helper import retrieve_kv_from_db_rows
from common.utils.exceptions import UserVisiableException, CommonException
from common.invokeCommand import InvokeCommand
from common.utils.asyc_utils import run_on_executor, run_callback
from common.helper import get_localhost_ip

class DBStatOpers(Abstract_Stat_Service):
    
    dba_opers = DBAOpers()
    confOpers = ConfigFileOpers()
    
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
    '''
    @todo: why use str_flag?
    '''    
    def stat(self, str_flag = ""):
        # if str_flag = "", then the request must come from the webport, not peer.
        if str_flag == "":
            rows_oper_dict = self._stat_rows_oper()
            # We check if the local database is in use.
            if (rows_oper_dict == {} or rows_oper_dict["oper_total"]["num_reads"] == 0):
                result_dict = self.get_peer_wsrep_status()
                logging.info("rows_oper_dict:" + str(rows_oper_dict))
            # When local database is in use, we go on processing in this node.
            else:
                wsrep_status_dict = self._stat_wsrep_status()
                innodb_buffer_dict = self._stat_innodb_buffer()
                variable_status_dict = self._stat_variable_status()
        else:
            #This str_flag  must be "inner", This process the request from peer nodes.
            wsrep_status_dict = self._stat_wsrep_status()
            rows_oper_dict = self._stat_rows_oper()
            innodb_buffer_dict = self._stat_innodb_buffer()
            variable_status_dict = self._stat_variable_status()
        
        # If we find that the local database is not in use, then the all results come from peer node.
        if (rows_oper_dict == {} or rows_oper_dict["oper_total"]["num_reads"] == 0):
            return result_dict
        
        result = {}
        # Else we know that local database in in use, we return it in the original way.   
        result.setdefault('wsrep_status_dict',wsrep_status_dict)
        result.setdefault('rows_oper',rows_oper_dict)
        result.setdefault('innodb_buffer',innodb_buffer_dict)
        result.setdefault('variable_status',variable_status_dict)
        return result
    
    def get_peer_wsrep_status(self):
        logging.info("can not connect to local database site")
        
        cluster_started_nodes = self.zkOper.retrieve_started_nodes()
        
        confDict = self.confOpers.getValue(options.data_node_property, ['dataNodeIp'])
        local_ip = confDict['dataNodeIp']

        logging.info("local ip:" + str(local_ip))
        if cluster_started_nodes.count(local_ip) != 0:
            cluster_started_nodes.remove(local_ip)
        logging.info("candicates are: " + str(cluster_started_nodes))    
        result = ""
        for ip in cluster_started_nodes:
            url_post = "/db/all/stat?inner=true"
            result = self.communicate(ip, url_post)
            logging.info("origin result: " + str(result))
            logging.info(result.replace("\\", ""))
            if result.count("wsrep_status_dict") != 0:
                break
        if result.count("wsrep_status_dict") != 0:
            result_dict = json.loads(result)
            return result_dict["response"]
        else:
            raise CommonException("Can\'t connect to mysql server")
    
    
    def _stat_wsrep_status(self):
        conn = self.dba_opers.get_mysql_connection()
        if conn is None :
            raise CommonException("Can\'t connect to mysql server")   
         
        try:
            rows = self.dba_opers.show_status(conn)
        finally:
            conn.close()
    
        key_value = retrieve_kv_from_db_rows(rows,['wsrep_flow_control_paused',\
                                                   'wsrep_flow_control_sent',\
                                                   'wsrep_local_recv_queue_avg',\
                                                   'wsrep_local_send_queue_avg'])
    
        slowest_node_param_dict={}
        slowest_node_param_dict.setdefault('wsrep_flow_control_sent',key_value.get('wsrep_flow_control_sent'))
        slowest_node_param_dict.setdefault('wsrep_local_recv_queue_avg', key_value.get('wsrep_local_recv_queue_avg'))
    
        result = {}
        result.setdefault('wsrep_flow_control_paused', key_value.get('wsrep_flow_control_paused'))
        result.setdefault('slowest_node_param', slowest_node_param_dict)
        result.setdefault('wsrep_local_send_queue_avg', key_value.get('wsrep_local_send_queue_avg'))
    
        return result
    
    def communicate(self, peer_ip, url):
        http_client = tornado.httpclient.HTTPClient()
        requesturi = "http://"+peer_ip+":"+str(options.port)+url
        try:
            response = http_client.fetch(requesturi)
        except tornado.httpclient.HTTPError as e:
            logging.error(str(e))
            http_client.close()
            return "error"
        logging.info(str(response.body))
        return response.body

    def stat_wsrep_status_flow_control_paused(self):
        status_dict = self._stat_wsrep_status()
        value = status_dict.get('wsrep_flow_control_paused')
        return {'wsrep_flow_control_paused': value}
    
    def stat_wsrep_status_slowest_node_param(self):
        status_dict = self._stat_wsrep_status()
        sub_dict = status_dict.get('slowest_node_param')
        return sub_dict
    
    def stat_wsrep_status_slowest_network_param(self):
        status_dict = self._stat_wsrep_status()
        value = status_dict.get('wsrep_local_send_queue_avg')
        return {'wsrep_local_send_queue_avg': value}
    
    @run_on_executor()
    @run_callback
    def stat_binlog_eng_log_pos(self, params):
        if not params:
            raise UserVisiableException('params are not given')
     
        conn = self.dba_opers.get_mysql_connection()
        if None == conn:
            raise UserVisiableException("Can\'t connect to mysql server")
        
        try:
            cursor = conn.cursor()
            cursor.execute('show binary logs')
            rows_bin_logs = cursor.fetchall()
            assert rows_bin_logs
            invokecommand = InvokeCommand()
            for i in range(len(rows_bin_logs)):
                master_log_file = rows_bin_logs[-i-1][-2]
                ret_str = invokecommand._runSysCmd('''mysql -uroot -pMcluster -e "show binlog events IN '%s'"|grep 'xid=%s' '''%(master_log_file, params['xid']))
                assert ret_str               
                if ret_str[0]:
                    break
            
            end_log_pos = ret_str[0].strip('\n').split('\t')[-2]
        
        except AssertionError, e:
            logging.info(e)
        finally:
            conn.close()
            
        result = {}
        result.setdefault('Master_Log_File', master_log_file)
        result.setdefault('End_Log_Pos', end_log_pos)
        return result
    
    @run_on_executor()
    @run_callback
    def bin_log_node_stat(self):
        conn = self.dba_opers.get_mysql_connection()
        if None == conn:
            raise UserVisiableException("Can\'t connect to mysql server")        
        try:
            cursor = conn.cursor()
            cursor.execute("show variables like 'log_bin'")
            rows_stat_log_bin = cursor.fetchall()
            stat_log_bin = rows_stat_log_bin[0][1]
        finally:
            conn.close()
        
        zkOper = self.retrieve_zkOper()
        started_node_list = zkOper.retrieve_started_nodes()
        local_ip = get_localhost_ip()
        if local_ip in started_node_list:
            started_node_list.remove(local_ip)

        result = {}
        result.setdefault('node_list', started_node_list)
        result.setdefault('stat_log_bin', stat_log_bin)
        return result

    def _stat_rows_oper(self ):
        processor_existed = self._check_mysql_processor_exist()
        
        result = {}
        if not processor_existed:
            return result
        
        target_dict = self._retrieve_dict_with_result(options.stat_rows_oper)
        
        key_list = ['num_updates','num_reads','num_deletes','num_inserts']
        oper_total_dict = self._split_key_value(key_list, target_dict)
        key_list = ['num_reads_sec','num_updates_sec','num_deletes_sec','num_inserts_sec']
        oper_per_second_dict = self._split_key_value(key_list, target_dict)        
        result.setdefault("oper_total", oper_total_dict)
        result.setdefault("oper_per_second", oper_per_second_dict)
        
        return result
    
    def stat_rows_oper_total(self):
        oper_dict = self._stat_rows_oper()
        sub_dict = oper_dict.get('oper_total')
        return sub_dict
    
    def stat_rows_oper_per_second(self):
        oper_dict = self._stat_rows_oper()
        sub_dict = oper_dict.get('oper_per_second')
        return sub_dict
    
    
    def _stat_innodb_buffer(self):
        processor_existed = self._check_mysql_processor_exist()
        
        result = {}
        if not processor_existed:
            return result
        
        target_dict = self._retrieve_dict_with_result(options.stat_innodb_buffer)
        
        key_list = ['total_mem_alloc','add_pool_alloc']
        mem_alloc_dict = self._split_key_value(key_list, target_dict)
        key_list = ['pages_total','pages_modified']
        page_dict = self._split_key_value(key_list, target_dict)
        key_list = ['buf_pool_size','buf_pool_hit_rate','buf_free']
        buffer_pool_dict = self._split_key_value(key_list, target_dict)
        
        value = buffer_pool_dict.get('buf_pool_hit_rate')
        if value == '--' or value == '' or value == 0:
            value = 0
        else:
            buf_pool_hit_rate_list = value.split('/')
            value = int(buf_pool_hit_rate_list[0])/int(buf_pool_hit_rate_list[1])
            
        buffer_pool_dict['buf_pool_hit_rate'] = str(value)
        
        result.setdefault("mem_alloc", mem_alloc_dict)
        result.setdefault("page", page_dict)
        result.setdefault("buffer_pool", buffer_pool_dict)
        
        return result
    
    def stat_innodb_buffer_mem_alloc(self):
        buffer_dict = self._stat_innodb_buffer()
        sub_dict = buffer_dict.get('mem_alloc')
        return sub_dict
    
    def stat_innodb_buffer_page(self):
        buffer_dict = self._stat_innodb_buffer()
        sub_dict = buffer_dict.get('page')
        return sub_dict
    
    def stat_innodb_buffer_buffer_pool(self):
        buffer_dict = self._stat_innodb_buffer()
        sub_dict = buffer_dict.get('buffer_pool')
        return sub_dict
        
    def _stat_variable_status(self):
        processor_existed = self._check_mysql_processor_exist()
        
        result = {}
        if not processor_existed:
            return result
        
        target_dict = self._retrieve_dict_with_result(options.stat_variable_status)
        
        key_list = ['Opens_PS','QPS','Commit_PS','Threads_PS']
        ps_dict = self._split_key_value(key_list, target_dict)
        key_list = ['Thread_Cache_Used','CXN_Used_Ever','CXN_Used_Now','Table_Cache_Used']
        used_dict = self._split_key_value(key_list, target_dict)
        key_list = ['R_W_Ratio','Rollback_Commit','Write_Commit']
        ratio_dict = self._split_key_value(key_list, target_dict)
        
        result.setdefault("ps", ps_dict)
        result.setdefault("used", used_dict)
        result.setdefault("ration", ratio_dict)
        
        return result
    
    def stat_variable_status_ps(self):
        status_dict = self._stat_variable_status()
        sub_dict = status_dict.get('ps')
        return sub_dict
    
    def stat_variable_status_used(self):
        status_dict = self._stat_variable_status()
        sub_dict = status_dict.get('used')
        return sub_dict
    
    def stat_variable_status_ration(self):
        status_dict = self._stat_variable_status()
        sub_dict = status_dict.get('ration')
        return sub_dict

'''
Created on 2013-7-21

@author: asus
'''
from common.dba_opers import DBAOpers
from tornado.options import options
from common.abstract_stat_service import Abstract_Stat_Service
from common.helper import retrieve_kv_from_db_rows
from common.utils.exceptions import HTTPAPIError

class DBStatOpers(Abstract_Stat_Service):
    
    dba_opers = DBAOpers()
    
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        
    def stat(self):
        rows_oper_dict = self._stat_rows_oper()
        innodb_buffer_dict = self._stat_innodb_buffer()
        variable_status_dict = self._stat_variable_status()
        wsrep_status_dict = self._stat_wsrep_status()
        
        dict = {}
        dict.setdefault('rows_oper',rows_oper_dict)
        dict.setdefault('innodb_buffer',innodb_buffer_dict)
        dict.setdefault('variable_status',variable_status_dict)
        dict.setdefault('wsrep_status_dict',wsrep_status_dict)
        
        return dict
    
    def _stat_wsrep_status(self):
        conn = self.dba_opers.get_mysql_connection()
        
        if conn is None:
            raise HTTPAPIError(status_code = 417, error_detail="no way to retrieve the db connection",
                                notification = "direct", 
                                log_message= "no way to retrieve the db connection",
                                response =  "no way to retrieve the db connection")
        
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
        
        dict = {}
        dict.setdefault('wsrep_flow_control_paused', key_value.get('wsrep_flow_control_paused'))
        dict.setdefault('slowest_node_param', slowest_node_param_dict)
        dict.setdefault('wsrep_local_send_queue_avg', key_value.get('wsrep_local_send_queue_avg'))
        
        return dict
        
    def stat_wsrep_status_flow_control_paused(self):
        dict = self._stat_wsrep_status()
        value = dict.get('wsrep_flow_control_paused')
        return {'wsrep_flow_control_paused': value}
    
    def stat_wsrep_status_slowest_node_param(self):
        dict = self._stat_wsrep_status()
        sub_dict = dict.get('slowest_node_param')
        return sub_dict
    
    def stat_wsrep_status_slowest_network_param(self):
        dict = self._stat_wsrep_status()
        value = dict.get('wsrep_local_send_queue_avg')
        return {'wsrep_local_send_queue_avg': value}
        
    def _stat_rows_oper(self):
        processor_existed = self._check_mysql_processor_exist()
        
        dict = {}
        if not processor_existed:
            return dict
        
        target_dict = self._retrieve_dict_with_result(options.stat_rows_oper)
        
        key_list = ['num_updates','num_reads','num_deletes','num_inserts']
        oper_total_dict = self._split_key_value(key_list, target_dict)
        key_list = ['num_reads_sec','num_updates_sec','num_deletes_sec','num_inserts_sec']
        oper_per_second_dict = self._split_key_value(key_list, target_dict)
        
        dict.setdefault("oper_total", oper_total_dict)
        dict.setdefault("oper_per_second", oper_per_second_dict)
        
        return dict
    
    def stat_rows_oper_total(self):
        dict = self._stat_rows_oper()
        sub_dict = dict.get('oper_total')
        return sub_dict
    
    def stat_rows_oper_per_second(self):
        dict = self._stat_rows_oper()
        sub_dict = dict.get('oper_per_second')
        return sub_dict
    
    
    def _stat_innodb_buffer(self):
        processor_existed = self._check_mysql_processor_exist()
        
        dict = {}
        if not processor_existed:
            return dict
        
        target_dict = self._retrieve_dict_with_result(options.stat_innodb_buffer)
        
        key_list = ['total_mem_alloc','add_pool_alloc']
        mem_alloc_dict = self._split_key_value(key_list, target_dict)
        key_list = ['pages_total','pages_modified']
        page_dict = self._split_key_value(key_list, target_dict)
        key_list = ['buf_pool_size','buf_pool_hit_rate','buf_free']
        buffer_pool_dict = self._split_key_value(key_list, target_dict)
        
        value = buffer_pool_dict.get('buf_pool_hit_rate')
#        value = '1000 / 1000'
        if value == '--' or value == '' or value == 0:
            value = 0
        else:
            buf_pool_hit_rate_list = value.split('/')
            value = int(buf_pool_hit_rate_list[0])/int(buf_pool_hit_rate_list[1])
            
        buffer_pool_dict['buf_pool_hit_rate'] = str(value)
        
        dict.setdefault("mem_alloc", mem_alloc_dict)
        dict.setdefault("page", page_dict)
        dict.setdefault("buffer_pool", buffer_pool_dict)
        
        return dict
    
    def stat_innodb_buffer_mem_alloc(self):
        dict = self._stat_innodb_buffer()
        sub_dict = dict.get('mem_alloc')
        return sub_dict
    
    def stat_innodb_buffer_page(self):
        dict = self._stat_innodb_buffer()
        sub_dict = dict.get('page')
        return sub_dict
    
    def stat_innodb_buffer_buffer_pool(self):
        dict = self._stat_innodb_buffer()
        sub_dict = dict.get('buffer_pool')
        return sub_dict
        
    def _stat_variable_status(self):
        processor_existed = self._check_mysql_processor_exist()
        
        dict = {}
        if not processor_existed:
            return dict
        
        target_dict = self._retrieve_dict_with_result(options.stat_variable_status)
        
        key_list = ['Opens_PS','QPS','Commit_PS','Threads_PS']
        ps_dict = self._split_key_value(key_list, target_dict)
        key_list = ['Thread_Cache_Used','CXN_Used_Ever','CXN_Used_Now','Table_Cache_Used']
        used_dict = self._split_key_value(key_list, target_dict)
        key_list = ['R_W_Ratio','Rollback_Commit','Write_Commit']
        ratio_dict = self._split_key_value(key_list, target_dict)
        
        dict.setdefault("ps", ps_dict)
        dict.setdefault("used", used_dict)
        dict.setdefault("ration", ratio_dict)
        
        return dict
    
    def stat_variable_status_ps(self):
        dict = self._stat_variable_status()
        sub_dict = dict.get('ps')
        return sub_dict
    
    def stat_variable_status_used(self):
        dict = self._stat_variable_status()
        sub_dict = dict.get('used')
        return sub_dict
    
    def stat_variable_status_ration(self):
        dict = self._stat_variable_status()
        sub_dict = dict.get('ration')
        return sub_dict

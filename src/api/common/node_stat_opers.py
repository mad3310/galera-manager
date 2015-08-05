'''
Created on 2013-7-21

@author: asus
'''
import logging
from tornado.options import options
from common.abstract_stat_service import Abstract_Stat_Service
from common.helper import is_monitoring, get_localhost_ip
from common.helper.mysql_shell_dict import MYSQL_SHELL_DICT
from common.invokeCommand import InvokeCommand
from common.utils.exceptions import UserVisiableException

class NodeStatOpers(Abstract_Stat_Service):
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
    
    def stat(self):
        mysql_dir_size_partion = self.stat_data_dir_size()
        mysql_top_partion = self._stat_mysql_top()
        node_mem_size = self.stat_node_memory()
        
        result = {}
        result.setdefault("dir_size_partion", mysql_dir_size_partion)
        result.setdefault('mysql_top_partion', mysql_top_partion)
        result.setdefault('node_mem_size', node_mem_size)
        
        return result
        
    def stat_data_dir_size(self):
        result = {}
        
        return_result = self.invokeCommand.run_check_shell(options.stat_dir_size)
        df_output_lines = [s.split() for s in return_result.splitlines()]
        
        for df_output_line in df_output_lines:
            used = df_output_line[4]
            mounted_on = df_output_line[5]
            used = used.replace('%','')
            if mounted_on == '/var' or mounted_on == '/srv/mcluster' or mounted_on == '/':
                result.setdefault(mounted_on,used)
        
        return result
    
    def _stat_mysql_top(self):
        result = {}
        
        zkOper = self.retrieve_zkOper()
        
        if not is_monitoring(get_localhost_ip(), zkOper):
            result.setdefault('mysql_cpu_partion', 0.0)
            result.setdefault('mysql_mem_partion', 0.0)
            return result
        
        return_result = self.invokeCommand.run_check_shell(options.stat_top_command)
        logging.info("return_result :" + str(return_result))
        
        mysql_info_list = []
        try: 
            mysql_info_list = return_result.split('\n\n\n')[0].split('\n')[7].split()
        except IndexError:
            logging.info("mysql pid not found through top -umysql")
        if mysql_info_list is None or mysql_info_list == []:
            mysql_cpu = 0.0
            mysql_mem = 0.0
        else:
            mysql_cpu = mysql_info_list[8]
            mysql_mem = mysql_info_list[9]
        
        result.setdefault('mysql_cpu_partion', mysql_cpu)
        result.setdefault('mysql_mem_partion', mysql_mem)
        
        return result
    
    def stat_mysql_cpu(self):
        top_dict = self._stat_mysql_top()
        value = top_dict.get('mysql_cpu_partion')
        return {'mysql_cpu_partion': value}
    
    def stat_mysql_memory(self):
        _top_dict = self._stat_mysql_top()
        value = _top_dict.get('mysql_mem_partion')
        return {'mysql_mem_partion': value}
    
    def stat_node_memory(self):
        
        return_result = self.invokeCommand.run_check_shell(options.stat_mem_command)
        mysql_mem_list = return_result.split('\n\n\n')[0].split('\n')[2].split()
        
        if mysql_mem_list is None:
            node_mem_used_size = 0.0
            node_mem_free_size = 0.0
        else:
            node_mem_used_size = mysql_mem_list[2]
            node_mem_free_size = mysql_mem_list[3]
        
        result = {}    
        result.setdefault('node_mem_used_size', node_mem_used_size)
        result.setdefault('node_mem_free_size', node_mem_free_size)
        
        return result

    
class NodeStatDetailOpers(Abstract_Stat_Service):
    '''
    classdocs
    '''
    
    invoke_command = InvokeCommand()

    def node_stat_detail_info(self, param_dicts):
        return self.__get_db_monitor_res(param_dicts)

    def __get_db_monitor_res(self, param_dicts):
        ret_dict = {}
        if not param_dicts:
            raise UserVisiableException('params are not given')
        
        for param in param_dicts:
            if param not in MYSQL_SHELL_DICT:
                raise UserVisiableException('%s param given is wrong!' %param)
            mysql_database_name = param_dicts[param]            
            if param == 'stat_database_size_command':
                self._get_stat_database_size_res(param, mysql_database_name, ret_dict)

            elif param == 'stat_table_space_analyze_command':
                self._get_stat_table_space_analyze_res(param, mysql_database_name, ret_dict)

            else:
                self._get_stat_common_detail_res(param, ret_dict)
                  
        return ret_dict

    def _get_stat_database_size_res(self, param, name, mysql_dict):
        sql_result = self.invoke_command._runSysCmd(MYSQL_SHELL_DICT.get(param).format(name))
        if sql_result[1]:
            raise UserVisiableException(sql_result[0])
        sql_list = sql_result[0].strip('\n').split('\t')[::-1]
        mysql_dict.setdefault(param, sql_list[1])

    def _get_stat_table_space_analyze_res(self, param, name, mysql_dict):
        sql_list_init, sql_list_dict = [], {}              
        sql_result = self.invoke_command._runSysCmd(MYSQL_SHELL_DICT.get(param).format(name))
        if sql_result[0]:
            sql_list_init = sql_result[0].strip('\n').split('\n')[1:]
            for item in sql_list_init:
                _dict = {}
                item_list=item.split('\t')
                _dict['table_comment'] = item_list[2]
                _dict['total_kb'] = item_list[3]
                sql_list_dict[item_list[1]] = _dict                
                mysql_dict.setdefault(param, sql_list_dict)
        else:
            raise UserVisiableException('database %s does not exist' %name)

        sql_list_init = sql_result[0].strip('\n').split('\n')[1:]
        for item in sql_list_init:
            _dict = {}
            item_list=item.split('\t')
            _dict['table_comment'] = item_list[2]
            _dict['total_kb'] = item_list[3]
            sql_list_dict[item_list[1]] = _dict                
            mysql_dict.setdefault(param, sql_list_dict)

    def _get_stat_common_detail_res(self, param, mysql_dict):        
        sql_result = self.invoke_command._runSysCmd(MYSQL_SHELL_DICT.get(param))
        if sql_result[1]:
            raise UserVisiableException(sql_result[0])
        sql_list = sql_result[0].strip('\n').split('\t')
        mysql_dict.setdefault(param, sql_list[1])

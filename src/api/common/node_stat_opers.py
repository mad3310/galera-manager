'''
Created on 2013-7-21

@author: asus
'''
from tornado.options import options
from common.invokeCommand import InvokeCommand
from common.abstract_stat_service import Abstract_Stat_Service

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
        
        dict = {}
        dict.setdefault("dir_size_partion", mysql_dir_size_partion)
        dict.setdefault('mysql_top_partion', mysql_top_partion)
        dict.setdefault('node_mem_size', node_mem_size)
        
        return dict
        
    def stat_data_dir_size(self):
        dict = {}
        
        return_result = self.invokeCommand.run_check_shell(options.stat_dir_size)
        df_output_lines = [s.split() for s in return_result.splitlines()]
        
        for df_output_line in df_output_lines:
            used = df_output_line[4]
            mounted_on = df_output_line[5]
            used = used.replace('%','')
            if mounted_on == '/var' or mounted_on == '/srv/mcluster' or mounted_on == '/':
                dict.setdefault(mounted_on,used)
        
        return dict
    
    def _stat_mysql_top(self):
        dict = {}
        
        return_result = self.invokeCommand.run_check_shell(options.stat_top_command)
        mysql_info_list = return_result.split('\n\n\n')[0].split('\n')[7].split()
        
        if mysql_info_list is None or mysql_info_list == []:
            mysql_cpu = 0.0
            mysql_mem = 0.0
        else:
            mysql_cpu = mysql_info_list[8]
            mysql_mem = mysql_info_list[9]
        
        dict.setdefault('mysql_cpu_partion', mysql_cpu)
        dict.setdefault('mysql_mem_partion', mysql_mem)
        
        return dict
    
    def stat_mysql_cpu(self):
        dict = self._stat_mysql_top()
        value = dict.get('mysql_cpu_partion')
        return {'mysql_cpu_partion': value}
    
    def stat_mysql_memory(self):
        dict = self._stat_mysql_top()
        value = dict.get('mysql_mem_partion')
        return {'mysql_mem_partion': value}
    
    def stat_node_memory(self):
        dict = {}
        
        return_result = self.invokeCommand.run_check_shell(options.stat_mem_command)
        mysql_mem_list = return_result.split('\n\n\n')[0].split('\n')[2].split()
        
        if mysql_mem_list is None:
            node_mem_used_size = 0.0
            node_mem_free_size = 0.0
        else:
            node_mem_used_size = mysql_mem_list[2]
            node_mem_free_size = mysql_mem_list[3]
            
        dict.setdefault('node_mem_used_size', node_mem_used_size)
        dict.setdefault('node_mem_free_size', node_mem_free_size)
        
        return dict

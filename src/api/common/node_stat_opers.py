'''
Created on 2013-7-21

@author: asus
'''
import logging
from tornado.options import options
from common.abstract_stat_service import Abstract_Stat_Service
from common.helper import is_monitoring, get_localhost_ip, retrieve_directory_available, retrieve_directory_capacity

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
        result = {'/var':'0', '/srv/mcluster':'0', '/':'0'}
        
        #return_result = self.invokeCommand.run_check_shell(options.stat_dir_size)
        #df_output_lines = [s.split() for s in return_result.splitlines()]
        
        #for df_output_line in df_output_lines:
            #used = df_output_line[4]
            #mounted_on = df_output_line[5]
            #used = used.replace('%','')
            #if mounted_on == '/var' or mounted_on == '/srv/mcluster' or mounted_on == '/':
                #result.setdefault(mounted_on,used)
        
        return result
    
    def _stat_mysql_top(self):
        result = {}
        
        #zkOper = self.retrieve_zkOper()
        
        #if not is_monitoring(get_localhost_ip(), zkOper):
            #result.setdefault('mysql_cpu_partion', 0.0)
            #result.setdefault('mysql_mem_partion', 0.0)
            #return result
        
        #return_result = self.invokeCommand.run_check_shell(options.stat_top_command)
        #logging.info("return_result :" + str(return_result))
        
        mysql_info_list = []
        #try: 
            #mysql_info_list = return_result.split('\n\n\n')[0].split('\n')[7].split()
        #except IndexError:
            #logging.info("mysql pid not found through top -umysql")
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
        
        #return_result = self.invokeCommand.run_check_shell(options.stat_mem_command)
        #mysql_mem_list = return_result.split('\n\n\n')[0].split('\n')[2].split()
        mysql_mem_list = []
        
        if mysql_mem_list is None or mysql_mem_list == []:
            node_mem_used_size = 0.0
            node_mem_free_size = 0.0
        else:
            node_mem_used_size = mysql_mem_list[2]
            node_mem_free_size = mysql_mem_list[3]
        
        result = {}    
        result.setdefault('node_mem_used_size', node_mem_used_size)
        result.setdefault('node_mem_free_size', node_mem_free_size)
        
        return result
    
    def stat_node_zk_address(self):
        _zk_infos = []
        zkAddress = ''
        zkPort = ''
        zkLeader = ''
        with open('/opt/letv/mcluster-manager/api/config/mclusterManager.cnf', 'r') as f:
            _zk_infos = f.readlines()
        
        zkAddress = _zk_infos[0].split('=')[1].strip('\n')
        zkPort = _zk_infos[1].split('=')[1].strip('\n')
        zkLeader = _zk_infos[2].split('=')[1].strip('\n')
  
        return {'zkAddress': zkAddress, 'zkPort': zkPort, 'zkLeader': zkLeader}

    def stat_data_dir_available(self):
        '''
        '''
        _srv_mcluster_available = retrieve_directory_available('/srv/mcluster')
        _srv_mcluster_total = retrieve_directory_capacity('/srv/mcluster')
        _data_directory_available = retrieve_directory_available('/data')
        
        result = {
            "srv_mcluster_available" : _srv_mcluster_available,
            "srv_mcluster_total" : _srv_mcluster_total,
            "data_directory_available" : _data_directory_available
        }
        return result
    
    def stat_data_mem_available(self):
        mem_stat = {}
        with open("/proc/meminfo",'ro') as f:
            con = f.read().split()
        mem_stat['MemTotal'] = con[0]
        mem_stat['MemFree'] = con[1]

        return mem_stat 
    
    def stat_work_load(self):
        loadavg = {}
        with open("/proc/loadavg",'ro') as f:
            con = f.read().split()
        
        loadavg['loadavg_5'] = con[0]
        loadavg['loadavg_10'] = con[1]
        loadavg['loadavg_15'] = con[2]
        loadavg['nr'] = con[3]
        loadavg['last_pid'] = con[4]
        
        return loadavg

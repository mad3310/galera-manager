import logging

from common.invokeCommand import InvokeCommand
from abc import abstractmethod
from common.configFileOpers import ConfigFileOpers
from common.zkOpers import Abstract_ZkOpers
from tornado.options import options


class Abstract_Stat_Service(object):
    
    invokeCommand = InvokeCommand()
    
    confOpers = ConfigFileOpers()
    
    zkOper = None
    
    def __init__(self):
        '''
        Constructor
        '''
        
    def retrieve_zkOper(self):
        if None == self.zkOper:
            self.zkOper = Abstract_ZkOpers()
            
        return self.zkOper
        
    @abstractmethod
    def stat(self):
        raise NotImplementedError, "Cannot call abstract method"
        
    def _retrieve_dict_with_result(self, stat_command):
        return_result = self.invokeCommand.run_check_shell(stat_command)
        title_value_result = return_result.split('\n')
       
        
        title_list = title_value_result[0].split('\t')
        value_list = []
        if len(title_value_result) == 2:
            value_list = title_value_result[1].split('\t') 
           
        result = {}
        for i in range(len(title_list)):
            title = title_list[i]
            if value_list == [] or value_list is None:
                value = 0
            elif value_list[0] == "127.0.0.1: Can't connect to MySQL server on '127.0.0.1' (111)":
                value = 0
            elif value_list[0] == '':
                value = 0
            else:
                value = value_list[i]
            result.setdefault(title,value)
        
        return result
    
    def _convert_dict_to_str(self, param):
        title_list_str = ''
        value_list_str = ''
        for (key, value) in param.iteritems():
            title_list_str += key + '\t'
            value_list_str += value + '\t'
            
        title_list_str = title_list_str.rstrip('\t')
        value_list_str = value_list_str.rstrip('\t')
        
        return_result = """{title_list_str}\n{value_list_str}""".format(title_list_str=title_list_str,
                                                                        value_list_str=value_list_str)
        return return_result
    
            
    def _split_key_value(self, key_list, target_dict):
        sub_dict = {}
        
        for i in range(len(key_list)):
            title = key_list[i]
            value = target_dict.get(title)
            sub_dict.setdefault(title,value)
            
        return sub_dict
    
    def _check_mysql_processor_exist(self):
        zkOper = Abstract_ZkOpers()
        try:
            started_nodes = zkOper.retrieve_started_nodes()
        finally:
            zkOper.stop()
            
        logging.info("close zk client connection successfully") 
        confDict = self.confOpers.getValue(options.data_node_property, ['dataNodeIp'])
        data_node_ip = confDict['dataNodeIp']
        
        processor_existed = False
        
        for started_node in started_nodes:
            if started_node == data_node_ip:
                processor_existed = True
                break
            
        return processor_existed

'''
Created on 2016-01-11

@author: xu
'''
import logging

class BackupEvlScore(object):

    weight_item_score = {'memory': 10.0, 'disk': 30.0, 'load5': 30.0, 'load10': 20.0, 'load15': 10.0 }
    
    def __init__(self):
        self.load5_score_dict, self.load10_score_dict, self.load15_score_dict = {}, {}, {}
        self.memory_score_dict = {}
        self.space_score_dict = {}
        self.host_score_dict = {} 

    def _analysis_usable_backup_node(self, system_loads, free_spaces, free_memory):
        system_loads_load5_list, system_loads_load10_list, system_loads_load15_list = [], [], []

        memory_list = []
        disk_dict = {}
        disk_max_value = 0
        
        [system_loads_load5_list.append(float(system_loads[i]['loadavg_5'])) for i in system_loads]
        [system_loads_load10_list.append(float(system_loads[i]['loadavg_10'])) for i in system_loads]
        [system_loads_load15_list.append(float(system_loads[i]['loadavg_15'])) for i in system_loads]
        [memory_list.append(float(free_memory[i]['MemFree'])) for i in free_memory]

        for k in free_spaces:
            total = float(free_spaces[k]['srv_mcluster_total'])
            avail = float(free_spaces[k]['srv_mcluster_available'])
            data_avail = float(free_spaces[k]['data_directory_available'])
            
            if avail > (total-avail):
                disk_dict[k] = avail
                if avail > disk_max_value:
                    disk_max_value = avail
        
        for i in system_loads:
            self.load5_score_dict[i] = self.weight_item_score['load5'] * max(system_loads_load5_list)/(float(system_loads[i]['loadavg_5'])+0.000001)
            self.load10_score_dict[i] = self.weight_item_score['load10'] * max(system_loads_load10_list)/(float(system_loads[i]['loadavg_10'])+0.000001)
            self.load15_score_dict[i] = self.weight_item_score['load15'] * max(system_loads_load15_list)/(float(system_loads[i]['loadavg_15'])+0.000001)

        for j in free_memory:
            self.memory_score_dict[j] = (self.weight_item_score['memory']*float(free_memory[j]['MemFree']))/max(memory_list)
        
        for k in disk_dict:
            if disk_max_value:
                self.space_score_dict[k] = (self.weight_item_score['disk']*disk_dict[k])/disk_max_value
               
        self.__get_item_score() 
           
        return self._get_backup_node_ip()
        
    
    def _get_backup_node_ip(self):
        host_score_list = sorted(self.host_score_dict.items(), key=lambda i: i[1], reverse=True)
        host_list = [i[0] for i in host_score_list]

        logging.info('select host list :%s' % str(host_list))
        return host_list
        
    def __get_item_score(self):
        for _score_ip in self.space_score_dict:
            if self.space_score_dict[_score_ip]:
                self.host_score_dict[_score_ip] = self.memory_score_dict[_score_ip] + self.space_score_dict.get(_score_ip) \
                + self.load5_score_dict[_score_ip] + self.load10_score_dict[_score_ip] + self.load15_score_dict[_score_ip]

            else:
                logging.info("The disk is full")

    

# coding:utf-8
'''
Created on 2016-01-11

@author: xu
'''
import logging


class BackupEvlScore(object):

    item_weight = {'memory': 20.0, 'disk': 30.0, 'load1': 20.0, 'load5': 20.0, 'load15': 10.0}

    def _analysis_usable_backup_node(self, system_loads, free_spaces, free_memory):
        # 获取各项指标的最大值，作为打分的参考标准
        loadavg_1_max = max([float(n['loadavg_1']) for n in system_loads.itervalues()])
        loadavg_5_max = max([float(n['loadavg_5']) for n in system_loads.itervalues()])
        loadavg_15_max = max([float(n['loadavg_15']) for n in system_loads.itervalues()])
        memory_max = max([float(n['MemFree']) for n in free_memory.itervalues()])
        disk_avail_max = max([float(n['srv_mcluster_available']) for n in free_spaces.itervalues()])

        # 获取各节点loadavg不同周期的分数，loadavg值越低，得分越高
        loadavg_values_1, loadavg_values_5, loadavg_values_15 = {}, {}, {}
        for node_ip, loadavg in system_loads.iteritems():
            loadavg_values_1[node_ip] = self.item_weight['load1'] * (1 - float(loadavg['loadavg_1']) / loadavg_1_max)
            loadavg_values_5[node_ip] = self.item_weight['load5'] * (1 - float(loadavg['loadavg_5']) / loadavg_5_max)
            loadavg_values_15[node_ip] = self.item_weight['load15'] * (1 - float(loadavg['loadavg_15']) / loadavg_15_max)

        # 获取各节点剩余内存的分数，可用内存越大，得分越高
        memory_values = {}
        for node_ip, meminfo in free_memory.iteritems():
            memory_values[node_ip] = self.item_weight['memory'] * float(meminfo['MemFree']) / memory_max

        # 获取各节点剩余磁盘容量的分数，容量越大，得分越高
        disk_values = {}
        for node_ip, diskinfo in free_spaces.iteritems():
            disk_values[node_ip] = self.item_weight['disk'] * float(diskinfo['srv_mcluster_available']) / disk_avail_max

        # 计算每个节点的总分
        nodes_score = {}
        for node_ip in disk_values:
            nodes_score[node_ip] = sum([loadavg_values_1[node_ip],
                                        loadavg_values_5[node_ip],
                                        loadavg_values_15[node_ip],
                                        memory_values[node_ip],
                                        disk_values[node_ip]])
        # 按总分倒序排列后的节点IP列表
        nodes_sorted_by_score = sorted(nodes_score, key=nodes_score.get, reverse=True)
        logging.info('selected backup nodes:%s' % str(nodes_sorted_by_score))
        return nodes_sorted_by_score

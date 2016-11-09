# -*- coding: utf-8 -*-

import os

from common.abstract_stat_service import Abstract_Stat_Service
from common.utils import disk_available, disk_capacity, dir_size


class NodeStatOpers(Abstract_Stat_Service):
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
        """这个方法目前已被不明人士注释，后续重构考虑删除  chenwenquan"""
        result = {'/var': '0', '/srv/mcluster': '0', '/': '0'}

        # return_result = self.invokeCommand.run_check_shell(options.stat_dir_size)
        # df_output_lines = [s.split() for s in return_result.splitlines()]

        # for df_output_line in df_output_lines:
        #     used = df_output_line[4]
        #     mounted_on = df_output_line[5]
        #     used = used.replace('%', '')
        # if mounted_on == '/var' or mounted_on == '/srv/mcluster' or mounted_on == '/':
        #     result.setdefault(mounted_on, used)

        return result

    def _stat_mysql_top(self):
        result = {}

        # zkOper = self.retrieve_zkOper()

        # if not is_monitoring(get_localhost_ip(), zkOper):
        #     result.setdefault('mysql_cpu_partion', 0.0)
        #     result.setdefault('mysql_mem_partion', 0.0)
        #     return result

        # return_result = self.invokeCommand.run_check_shell(options.stat_top_command)
        # logging.info("return_result :" + str(return_result))

        mysql_info_list = []
        # try:
        #     mysql_info_list = return_result.split('\n\n\n')[0].split('\n')[7].split()
        # except IndexError:
        #     logging.info("mysql pid not found through top -umysql")
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

        # return_result = self.invokeCommand.run_check_shell(options.stat_mem_command)
        # mysql_mem_list = return_result.split('\n\n\n')[0].split('\n')[2].split()
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
        result = {}
        with open('/opt/letv/mcluster-manager/api/config/mclusterManager.cnf', 'r') as f:
            for line in f.readlines():
                k, v = line.strip().split('=')
                result.setdefault(k, v)
        return result

    def stat_data_disk_available(self):
        _srv_mcluster_available = disk_available('/srv/mcluster') / (1024.0 * 1024 * 1024)
        _srv_mcluster_total = disk_capacity('/srv/mcluster') / (1024.0 * 1024 * 1024)
        _data_directory_available = disk_available('/data') / (1024.0 * 1024 * 1024)

        result = {
            "srv_mcluster_available": _srv_mcluster_available,
            "srv_mcluster_total": _srv_mcluster_total,
            "data_directory_available": _data_directory_available
        }
        return result

    def stat_data_mem_available(self):
        mem_stat = {}
        with open("/proc/meminfo", 'r') as f:
            con = f.readlines()

        mem_stat['MemTotal'] = con[0].split()[1]
        mem_stat['MemFree'] = con[1].split()[1]

        return mem_stat

    def stat_work_load(self):
        """从 /proc/loadavg 文件获取系统平均负载。
        文件内容示例：

            0.78 0.96 1.02 4/4520 31150

        该文件含有如上5个字段的内容。前3个字段表示CPU和IO在最近1分钟、5分钟、
        15分钟周期内的利用率。第4个字段表示正在运行的进程数与总进程数。最后
        一个字段表示最近运行的进程ID。
        """
        names = ('loadavg_1', 'loadavg_5', 'loadavg_15', 'nr', 'last_pid')
        with open("/proc/loadavg", 'r') as f:
            values = f.read().strip().split()
        loadavg = dict(zip(names, values))
        return loadavg

    def stat_diskspace_enough_for_backup(self):
        """以磁盘空间判断当前节点是否可以称为备份节点的基本条件。与根据磁盘
        用量作为打分因素不冲突。

        '/data/mcluster_data': 远程备份目录
        '/srv/mcluster': 本地备份目录
        '/srv/mcluster/mysql': mysql数据目录

        (本地备份目录剩余空间 >= mysql数据目录大小*2  &&
         远程备份目录剩余空间 >= mysql数据目录大小*2)

        满足上述条件方可成为备份节点
        """
        backup_dir_local = '/srv/mcluster'
        backup_dir_remote = '/data/mcluster_data'
        mysql_data_dir = '/srv/mcluster/mysql'

        mysql_data_size = dir_size(mysql_data_dir)
        stat_local = os.statvfs(backup_dir_local)
        local_dir_avail = stat_local.f_bfree * stat_local.f_bsize
        stat_remote = os.statvfs(backup_dir_remote)
        remote_dir_avail = stat_remote.f_bfree * stat_remote.f_bsize

        can_backup = (local_dir_avail >= mysql_data_size * 2 and
                      remote_dir_avail >= mysql_data_size * 2)
        return {'diskspace_enough': can_backup}

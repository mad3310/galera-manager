#!/usr/bin/env python
# -*- coding: utf-8 -*-


from handlers.admin import (AdminConf, AdminUser, AdminReset)
from handlers.cluster import (CreateMCluster, InitMCluster, SyncMCluster, ClusterStart,
                              ClusterStop, ClusterStatus, ClusterZkRemove)
from handlers.dataNode import (DataNodeToMCluster, SyncDataNode,
                               DataNodeStart, DataNodeStop, DataNodeStat,
                               StatDataDirSize, DataNodeMonitorLogHealth,
                               DataNodeMonitorLogWarning, DateNodeConfZk,
                               PortCheck, StatDiskAvailable, StatMemAvailable,
                               StatMysqlCpuPartion, StatMysqlMemoryPartion,
                               StatNodeMemorySize, StatNodeWorkLoad,
                               StatNodeDiskEnough, DataNodeMonitorLogError)
from handlers.database import (BinLogNodestat, BinlogPos, DBCreate, DBDelete,
                               DDLBatch, DMLBatch, TablesRows, DBStat,
                               Inner_DB_Check_CurConns, Inner_DB_Check_User_CurConns,
                               Inner_DB_Check_WR, Inner_DB_Check_WsrepStatus,
                               Inner_DB_Retrieve_Recover_UUID_Seqno, StatInnoBufferMemAlloc,
                               StatInnoBufferPage, StatInnoBufferPool, StatMysqlInfo,
                               StatRowsOperPS, StatRowsOperTotal, StatVariableStatusPS,
                               StatVariableStatusRation, StatVariableStatusUsed,
                               StatWsrepStatusFlowControlPaused,
                               StatWsrepStatusSlowestNetworkParam,
                               StatWsrepStatusSlowestNodeParam)
from handlers.databaseUser import DBUser
from handlers.monitor import Mcluster_Monitor_Async, Mcluster_Monitor_Sync
from handlers.status import MclusterHealth, MclusterStatus, MclusterStatusDetail
from handlers.backup import BackUpCheck, BackUp_Checker, Backup, Inner_Backup_Action
from handlers.dump import DBDump, DumpCheck

# 管理配置
handlers = [
    (r"/admin/conf", AdminConf),
    (r"/admin/user", AdminUser),
    (r"/admin/reset", AdminReset),
    (r"/admin/reset", AdminReset)
]

# 集群操作及监控
handlers += [
    (r"/cluster", CreateMCluster),
    (r"/cluster/init", InitMCluster),
    (r"/cluster/sync", SyncMCluster),
    (r"/cluster/node", DataNodeToMCluster),
    (r"/cluster/start", ClusterStart),
    (r"/cluster/stop", ClusterStop),
    (r"/cluster/check/online_node", ClusterStatus),
    (r"/cluster/zk/remove", ClusterZkRemove)
]

# 集群监控
handlers += [
    (r"/mcluster/monitor", Mcluster_Monitor_Sync),
    (r"/mcluster/monitor/async", Mcluster_Monitor_Async),
    (r"/mcluster/status", MclusterStatus),
    (r"/mcluster/health", MclusterHealth),
    (r"/mcluster/status/([a-zA-Z]+)", MclusterStatusDetail)
]

# 节点操作及监控
handlers += [
    (r"/node/sync/([\.0-9]+)", SyncDataNode),
    (r"/node/start", DataNodeStart),
    (r"/node/stop", DataNodeStop),
    (r"/node/stat", DataNodeStat),
    (r"/node/stat/datadir/size", StatDataDirSize),
    (r"/node/stat/disk/available", StatDiskAvailable),
    (r"/node/stat/disk/enough", StatNodeDiskEnough),
    (r"/node/stat/memory/available", StatMemAvailable),
    (r"/node/stat/workload", StatNodeWorkLoad),
    (r"/node/stat/mysqlcpu/partion", StatMysqlCpuPartion),
    (r"/node/stat/mysqlmemory/partion", StatMysqlMemoryPartion),
    (r"/node/stat/memory/size", StatNodeMemorySize),
    (r"/node/conf/zk", DateNodeConfZk),
    (r"/node/stat/info", StatMysqlInfo),
    (r"/inner/node/check/log/error", DataNodeMonitorLogError),
    (r"/inner/node/check/log/warning", DataNodeMonitorLogWarning),
    (r"/inner/node/check/log/health", DataNodeMonitorLogHealth),
    (r"/inner/node_port/check", PortCheck),
]

# MYSQL操作及监控
handlers += [
    (r"/dbUser", DBUser),
    (r"/dbUser/([a-zA-Z\-\_0-9]+)/([a-zA-Z\-\_0-9]+)/([\.0-9\%]+|\%)", DBUser),
    (r"/db/create", DBCreate),
    (r"/db/([a-zA-Z\-\_0-9]+)/delete/", DBDelete),
    (r"/db/(?P<db_name>.*)/dump", DBDump),
    (r"/db/dump/file/(?P<file_name>.*)/check", DumpCheck),
    (r"/db/(?P<db_name>.*)/ddl/batch", DDLBatch),
    (r"/db/(?P<db_name>.*)/dml/batch", DMLBatch),
    (r"/db/(?P<db_name>.*)/tables/rows", TablesRows),
    # 监控信息收集
    (r"/db/binlog/pos", BinlogPos),
    (r"/db/binlog/node/stat", BinLogNodestat),
    (r"/db/all/stat", DBStat),
    (r"/db/all/stat/rowsoper/total", StatRowsOperTotal),
    (r"/db/all/stat/rowsoper/ps", StatRowsOperPS),
    (r"/db/all/stat/innobuffer/memallco", StatInnoBufferMemAlloc),
    (r"/db/all/stat/innobuffer/page", StatInnoBufferPage),
    (r"/db/all/stat/innobuffer/pool", StatInnoBufferPool),
    (r"/db/all/stat/variablestatus/ps", StatVariableStatusPS),
    (r"/db/all/stat/variablestatus/used", StatVariableStatusUsed),
    (r"/db/all/stat/variablestatus/ration", StatVariableStatusRation),
    (r"/db/all/stat/wsrepstatus/flow_control_paused", StatWsrepStatusFlowControlPaused),
    (r"/db/all/stat/wsrepstatus/slowest_node_param", StatWsrepStatusSlowestNodeParam),
    (r"/db/all/stat/wsrepstatus/slowest_network_param", StatWsrepStatusSlowestNetworkParam),
    (r"/inner/db/check/wr", Inner_DB_Check_WR),
    (r"/inner/db/check/wsrep_status", Inner_DB_Check_WsrepStatus),
    (r"/inner/db/check/cur_conns", Inner_DB_Check_CurConns),
    (r"/inner/db/check/cur_user_conns", Inner_DB_Check_User_CurConns),
    (r"/inner/db/recover/uuid_seqno", Inner_DB_Retrieve_Recover_UUID_Seqno),
]

# MYSQL备份
handlers += [
    (r"/backup", Backup),
    (r"/backup/check", BackUpCheck),
    (r"/backup/checker", BackUp_Checker),
    (r"/inner/backup", Inner_Backup_Action),
]

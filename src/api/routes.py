#!/usr/bin/env python
#-*- coding: utf-8 -*-


from handlers.base import *
from handlers.admin import *
from handlers.cluster import *
from handlers.dataNode import *
from handlers.database import *
from handlers.databaseUser import *
from handlers.monitor import *
from handlers.status import *
from handlers.backup import *
from handlers.arbitrator import *
handlers = [
            (r"/generateConfigFile", GenerateConfigFileHandler),
            (r"/copyConfigFileInfo", CopyConfigFileInfoHandler),
            (r"/admin/conf", AdminConf),
            (r"/admin/user", AdminUser),
            (r"/cluster", CreateMCluster),
            (r"/cluster/init", InitMCluster),
            (r"/cluster/sync", SyncMCluster),
            (r"/inner/admin/file/{filename}", DownloadFile),
            (r"/cluster/node", DataNodeToMCluster),
            (r"/cluster/start", ClusterStart),
            (r"/cluster/stop", ClusterStop),
            (r"/cluster/check/online_node", ClusterStatus),
            (r"/node/sync/([\.0-9]+)", SyncDataNode),
            (r"/node/start", DataNodeStart),
            (r"/node/stop", DataNodeStop),
            (r"/node/stat", DataNodeStat),
            (r"/node/stat/datadir/size", StatDataDirSize),
            (r"/node/stat/mysqlcpu/partion", StatMysqlCpuPartion),
            (r"/node/stat/mysqlmemory/partion", StatMysqlMemoryPartion),
            (r"/node/stat/memory/size", StatNodeMemorySize),
            (r"/node/stat/info", StatMysqlInfo),
            (r"/admin/reset", AdminReset),
            (r"/db", DBOnMCluster),
            (r"/db/([a-zA-Z\-\_0-9]+)", DBOnMCluster),
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
            (r"/dbUser", DBUser),
            (r"/dbUser/([a-zA-Z\-\_0-9]+)/([a-zA-Z\-\_0-9]+)/([\.0-9\%]+|\%)", DBUser),
            (r"/mcluster/monitor", Mcluster_Monitor_Sync),
            (r"/mcluster/monitor/async", Mcluster_Monitor_Async),
            (r"/mcluster/status", MclusterStatus),
            (r"/mcluster/health", MclusterHealth),
            (r"/mcluster/status/([a-zA-Z]+)", MclusterStatusDetail),
            (r"/inner/db/check/wr", Inner_DB_Check_WR),
            (r"/inner/db/check/wsrep_status", Inner_DB_Check_WsrepStatus),
            (r"/inner/db/check/cur_conns", Inner_DB_Check_CurConns),
            (r"/inner/db/check/cur_user_conns", Inner_DB_Check_User_CurConns),
            (r"/inner/db/recover/uuid_seqno", Inner_DB_Retrieve_Recover_UUID_Seqno),
            (r"/inner/node/check/log/error", DataNodeMonitorLogError),
            (r"/inner/node/check/log/warning", DataNodeMonitorLogWarning),
            (r"/inner/node/check/log/health", DataNodeMonitorLogHealth),
            (r"/inner/node_port/check", PortCheck),
            (r"/backup", BackUp),
            (r"/inner/backup", BackUper),
            (r"/backup/check", BackUpCheck),
            (r"/backup/checker", BackUp_Checker),
            (r"/backup/inner/check", BackUpChecker),
            (r"/arbitrator/node/start", ArbitratorStart), 
            (r"/inner/arbitrator/ip", ArbitratorIP)
#             (r"/arbitrator/check", ArbitratorCheck)
]

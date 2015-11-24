'''
Created on 2013-7-21

@author: asus
'''
import MySQLdb
import logging

from tornado.options import options
from common.helper import retrieve_kv_from_db_rows
from common.utils.exceptions import CommonException
from common.utils.exceptions import UserVisiableException
from common.utils.asyc_utils import run_on_executor, run_callback

class DBAOpers(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
        
        
    def __unicode__(self):
        return self.name
    
    def check_if_existed_database(self, conn, db_name):
        cursor = conn.cursor()
        sql = """SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{db_name}'""".format(db_name=db_name)
        cursor.execute(sql)
        rows = cursor.fetchall()
        logging.info("database:" + str(rows))
        if len(rows) == 0:
            return "false"
        return "true"
    
    def get_db_users(self, conn):
        cursor = conn.cursor()
        cursor.execute("""select * from mysql.user where user !='root' and user != 'sstuser' and user != 'monitor' and user != 'backup'""")
        rows = cursor.fetchall()
        logging.info(str(rows))
        return rows 
 
    def delete_user(self, conn, username, ip_address):
        cursor = conn.cursor()
        cursor.execute("""select count(*) as c from mysql.user where user='{username}' and host='{ip_address}'"""
                       .format(username=username,ip_address=ip_address))       
        rows = cursor.fetchall()
        c = rows[0][0]
        if c:
            sql = """drop user `{username}`@'{ip_address}'""".format(username=username,ip_address=ip_address)
            logging.info("drop user sql is:" + sql)
            cursor.execute(sql)
    
    def check_create_table(self, conn, tb_name, db_name):
        conn.select_db(db_name)
        cursor = conn.cursor()
        try:
            sql = "select * from `%s`" % (tb_name)
            cursor.execute(sql)
        except MySQLdb.Error:
            sql = "CREATE TABLE if not exists `%s` ( `id` int(12) NOT NULL AUTO_INCREMENT ,`time` varchar(32) NOT NULL, `identifier` varchar(64) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8" % (tb_name)
            cursor.execute(sql)
        logging.info('create table ' + tb_name + ' success')

    def insert_record_time(self, conn, time_data, identifier, tb_name, db_name):
        conn.select_db(db_name)
        cursor = conn.cursor()
        sql = "INSERT INTO `%s` (time, identifier ) VALUES('%s','%s')" % (tb_name, time_data, identifier)
        try:
            cursor.execute(sql)
        
        except Exception,e:
            logging.exception(e)
        conn.commit()
        logging.info('write record_time successfully')
        
    def query_count_rows(self, conn, tb_name, db_name):
        
        conn.select_db(db_name)
        cursor = conn.cursor()
        try:
            sql = "SELECT count(*) from `%s`" % (tb_name)
            cursor.execute(sql)
        except Exception,e:
            logging.exception(e)

        rows = cursor.fetchall()
        ret = rows[0][0]
        return ret
    
    def delete_tb_contents(self, conn, tb_name, db_name):
        conn.select_db(db_name)
        cursor = conn.cursor()
        try:
            sql = "truncate `%s`" % (tb_name)
            cursor.execute(sql)
        except Exception, e:
            logging.exception(e)
        
    def query_record_time(self, conn, identifier, tb_name, db_name):
        conn.select_db(db_name)
        cursor = conn.cursor()
        try:
            sql = "select time from `%s` where identifier = '%s' order by id desc limit 1" %(tb_name, identifier)
            cursor.execute(sql)
#             id = cursor.fetchall()
#             
#             sql = "SELECT time from %s where identifer = %s and id = %s" % (tb_name, identifer, id) 
#             cursor.execute(sql)
        except Exception, e:
            logging.exception(e)
        rows = cursor.fetchall()   
        ret = rows[0][0]
        return ret
    
    def check_tb_data(self, conn, tb_name, db_name):
        conn.select_db(db_name)
        cursor = conn.cursor()
        try :
            sql = "select count(*) from `%s`" % (tb_name)
            cursor.execute(sql)
            rows = cursor.fetchall()
            num = rows[0][0]
        
        except Exception, e :
            logging.error(e)
        return num
    

    def count_tb_table(self, conn, db_name):
        conn.select_db(db_name)
        cursor = conn.cursor()
        
        try :
            sql = "SHOW TABLES"
            cursor.execute(sql)            
            _tuples = cursor.fetchall()
            
            count = sum(1 for t in _tuples)
        except Exception, e:
            logging.exception(e)
        return count
    
    def drop_table(self, conn, tb_name, db_name ):
        conn.select_db(db_name)
        cursor = conn.cursor()
    
        try:
            sql = "DROP table `%s`" %(tb_name)
            cursor.execute(sql)
        except Exception, e:
            logging.exception(e)
        logging.info('drop ' + tb_name + 'success')
        
    def create_user(self, conn, username, passwd, ip_address='%',dbName = None):
        cursor = conn.cursor()
        cursor.execute("""select count(*) as c from mysql.user where user='{username}' and host='{ip_address}'"""
                       .format(username=username,ip_address=ip_address))        
        rows = cursor.fetchall()
        c = rows[0][0]
        
        if c and dbName == None:
            logging.info("user has existed, so you should be provide db name to grant privileges!")
            return
        
        if c:
            cursor.execute("""grant usage on `{dbName}`.* to `{username}`@'{ip_address}' identified by '{passwd}'"""
                           .format(dbName=dbName,username=username,passwd=passwd,ip_address=ip_address))
        else:
            cursor.execute("""CREATE USER `{username}`@'{ip_address}' IDENTIFIED BY '{passwd}'"""
                           .format(username=username,passwd=passwd,ip_address=ip_address))

                   
    def grant_wr_privileges(self, conn, username, passwd, database, ipAddress='%', 
                                max_queries_per_hour=0, 
                                max_updates_per_hour=0, 
                                max_connections_per_hour=0, 
                                max_user_connections=200):
        sql = """grant select, insert, update, delete, index, create temporary tables, execute, show view 
        on {database}.* to {username}@'{ipAddress}' identified 
        by '{passwd}' with 
        MAX_QUERIES_PER_HOUR {mqph} 
        MAX_UPDATES_PER_HOUR {muph} 
        MAX_CONNECTIONS_PER_HOUR {mcph} 
        MAX_USER_CONNECTIONS {muc}""".format(database=database,
                                             username=username,
                                             ipAddress=ipAddress,
                                             passwd=passwd,
                                             mqph=max_queries_per_hour,
                                             muph=max_updates_per_hour,
                                             mcph=max_connections_per_hour,
                                             muc=max_user_connections)
        logging.info('grant_wr_privileges:' + sql)
        cursor = conn.cursor()
        cursor.execute(sql)
        
    def grant_readonly_privileges(self, conn, username, passwd, database, ipAddress='%',
                                  max_queries_per_hour=0,
                                  max_connections_per_hour=0, 
                                  max_user_connections=200):
        max_updates_per_hour = 1
        cursor = conn.cursor()
        cursor.execute("""grant
        select, execute, show view on {database}.* to {username}@'{ipAddress}' identified 
        by '{passwd}' with 
        MAX_QUERIES_PER_HOUR {mqph} 
        MAX_UPDATES_PER_HOUR {muph} 
        MAX_CONNECTIONS_PER_HOUR {mcph} 
        MAX_USER_CONNECTIONS {muc}""".format(database=database,
                                             username=username,
                                             passwd=passwd,
                                             ipAddress=ipAddress,
                                             mqph=max_queries_per_hour,
                                             muph=max_updates_per_hour,
                                             mcph=max_connections_per_hour,
                                             muc=max_user_connections))
        
    def grant_manager_privileges(self, conn, username, passwd, database, ipAddress='%',
                                 max_queries_per_hour=0, 
                                 max_updates_per_hour=0, 
                                 max_connections_per_hour=0, 
                                 max_user_connections=200):
        cursor = conn.cursor()
        cursor.execute("""grant
        all privileges on {database}.* to {username}@'{ipAddress}' identified 
        by '{passwd}' with 
        MAX_QUERIES_PER_HOUR {mqph} 
        MAX_UPDATES_PER_HOUR {muph} 
        MAX_CONNECTIONS_PER_HOUR {mcph} 
        MAX_USER_CONNECTIONS {muc}""".format(database=database,
                                             username=username,
                                             passwd=passwd,
                                             ipAddress=ipAddress,
                                             mqph=max_queries_per_hour,
                                             muph=max_updates_per_hour,
                                             mcph=max_connections_per_hour,
                                             muc=max_user_connections
                                             ))
        
    def grant_resource_limit(self, conn, username, database, ip_address, 
                                           max_queries_per_hour=None, 
                                           max_updates_per_hour=None, 
                                           max_connections_per_hour=None, 
                                           max_user_connections=None):
        resource_limit_sql = """grant
                                usage on `{database}`.* to `{username}`@'{ip_address}' with 
                                MAX_QUERIES_PER_HOUR {mqph} 
                                MAX_UPDATES_PER_HOUR {muph} 
                                MAX_CONNECTIONS_PER_HOUR {mcph} 
                                MAX_USER_CONNECTIONS {muc}""".format(database=database,
                                                                     username=username,
                                                                     ip_address=ip_address,
                                                                     mqph=max_queries_per_hour,
                                                                     muph=max_updates_per_hour,
                                                                     mcph=max_connections_per_hour,
                                                                     muc=max_user_connections
                                                                     )
        cursor = conn.cursor()
        logging.info('the resource limit sql is:' + resource_limit_sql)
        cursor.execute(resource_limit_sql)
        
        
        
    def flush_privileges(self, conn):
        cursor = conn.cursor()         
        cursor.execute("flush privileges")
        
        
    def craete_database(self, conn, databaseName):
        cursor = conn.cursor()
        cursor.execute("create database if not exists `%s`" %databaseName)
        
    def drop_database(self, conn, databaseName):
        cursor = conn.cursor()
        cursor.execute("drop database if exists `%s`" %databaseName)
        
    def master_name(self):
        return "sa_%s"%(self.name)
    
    def readonly_name(self):
        return "sa_%s_ro"%(self.name)
    
    def account_info(self):
        return """
        database: %s<br />
        master_username: %s<br />
        master_password: %s<br />
        readonly_username: %s<br />
        readonly_password: %s
        """%(self.database,
             self.master_name(),
             self.master_passwd,
             self.readonly_name(),
             self.readonly_passwd)
        
    def show_status(self, conn):
        cursor = conn.cursor()
        cursor.execute("show status")
        rows = cursor.fetchall()
        return rows
    
    def show_processlist(self, conn):
        cursor = conn.cursor()
        cursor.execute("show processlist")
        rows = cursor.fetchall()
        return rows
    
    def show_variables(self, conn, like_item_name):
        cursor = conn.cursor()
        cursor.execute("show variables like '%" + like_item_name + "%'")
        rows = cursor.fetchall()
        return rows
    
    def show_user_max_conn(self, conn, username, host):
        cursor = conn.cursor()
        cursor.execute("select max_user_connections from mysql.user where user='{0}' and host='{1}';".format(username, host))
        rows = cursor.fetchall()
        if not rows:
            raise UserVisiableException('the %s user and %s host_ip is not exists' %(username, host))
        return rows[0][0]
    
    def show_user_current_conn(self, conn, username, host):
        cursor = conn.cursor()
        cursor.execute("select count(*) from information_schema.processlist where user='{0}' and host='{1}';".format(username, host))
        rows = cursor.fetchall()
        return rows[0][0]

    def check_existed_myisam_table(self, conn):
        cursor = conn.cursor()
        cursor.execute("""select count(1) 
                from information_schema.tables AS t
                where t.table_schema NOT IN ('information_schema','performance_schema','mysql','monitor') 
                and t.table_type = 'BASE TABLE' 
                and t.engine <> 'InnoDB'""")
        rows = cursor.fetchall()
        c = rows[0][0]
        return c
    
    def check_triggers(self, conn):
        cursor = conn.cursor()
        try:
            cursor.execute("""select count(1) from information_schema.triggers""")
        except Exception, e:
            logging.exception(e)
        rows = cursor.fetchall()
        c = rows[0][0]
        return c

    def check_existed_stored_procedure(self, conn):
        cursor = conn.cursor()
        cursor.execute("select count(1) from mysql.proc")
        rows = cursor.fetchall()
        logging.info(str(rows))
        c = rows[0][0]
        return c
    
    def check_existed_nopk(self, conn):
        cursor = conn.cursor()
        cursor.execute("""select count(1)
                from information_schema.tables AS t LEFT JOIN information_schema.key_column_usage AS c
                ON (t.table_schema = c.constraint_schema AND t.table_name = c.table_name)
                where t.table_schema NOT IN ('information_schema','performance_schema','mysql', 'monitor') 
                AND t.table_type = 'BASE TABLE' and c.constraint_name IS NULL""")
        rows = cursor.fetchall()
        c = rows[0][0]
        return c
    
    def check_existed_fulltext_and_spatial(self, conn):
        cursor = conn.cursor()
        cursor.execute("""select count(1)
                from information_schema.tables AS t LEFT JOIN information_schema.statistics AS s
                ON (t.table_schema = s.table_schema AND t.table_name = s.table_name
                AND s.index_type IN ('FULLTEXT','SPATIAL'))
                where t.table_schema NOT IN ('information_schema','performance_schema','mysql', 'monitor') 
                AND t.table_type = 'BASE TABLE' and s.index_type IN ('FULLTEXT','SPATIAL');""")
        rows = cursor.fetchall()
        c = rows[0][0]
        return c
        
    
    def check_anti_item(self, conn):
        cursor = conn.cursor()
        cursor.execute("""SELECT DISTINCT
               CONCAT(t.table_schema,'.',t.table_name) as tbl,
               t.engine,
               IF(ISNULL(c.constraint_name),'NOPK','') AS nopk,
               IF(s.index_type = 'FULLTEXT','FULLTEXT','') as ftidx,
               IF(s.index_type = 'SPATIAL','SPATIAL','') as gisidx
          FROM information_schema.tables AS t
              LEFT JOIN information_schema.key_column_usage AS c
                ON (t.table_schema = c.constraint_schema AND t.table_name = c.table_name
                    AND c.constraint_name = 'PRIMARY')
              LEFT JOIN information_schema.statistics AS s
                ON (t.table_schema = s.table_schema AND t.table_name = s.table_name
                    AND s.index_type IN ('FULLTEXT','SPATIAL'))
          WHERE t.table_schema NOT IN ('information_schema','performance_schema','mysql', 'monitor')
            AND t.table_type = 'BASE TABLE'
            AND (t.engine <> 'InnoDB' OR c.constraint_name IS NULL OR s.index_type IN ('FULLTEXT','SPATIAL'))
          ORDER BY t.table_schema,t.table_name;""")
        rows = cursor.fetchall()
        return rows
    
    def get_mysql_connection(self, host ='127.0.0.1', user="root", passwd='Mcluster', autocommit = True):
        conn = None
        
        try:
            conn=MySQLdb.Connect(host, user, passwd, port=options.mysql_port)
            conn.autocommit(autocommit)
        except Exception,e:
            logging.exception(e)
        
        return conn
    
    def get_databases(self):
        conn = self.get_mysql_connection()
        cursor = conn.cursor()
        cursor.execute("show databases")
        rows = cursor.fetchall()
        dbs = []
        for row in rows:
            db = row[0]
            if not db in ["mysql", "information_schema", "performance_schema"]:     
                yield (dbs,db)
                
    def retrieve_wsrep_status(self):
        conn = self.get_mysql_connection()
        
        if conn is None:
            return False
        
        try:
            rows = self.show_status(conn)
        finally:
            conn.close()
        
        key_value = retrieve_kv_from_db_rows(rows,['wsrep_ready',\
                                                   'wsrep_cluster_status',\
                                                   'wsrep_connected',\
                                                   'wsrep_local_state_comment',\
                                                   'wsrep_flow_control_paused',\
                                                   'wsrep_flow_control_sent',\
                                                   'wsrep_local_recv_queue_avg',\
                                                   'wsrep_local_send_queue_avg'])
        
        check_result = self._check_wsrep_ready(key_value)
        return check_result
        
    def _check_wsrep_ready(self,key_value):
        if key_value == {}:
            raise CommonException("the param should be not null")
        
        value = key_value.get('wsrep_ready')
        if 'ON' != value:
            logging.error("wsrep ready is " + value)
            return False
        
        value = key_value.get('wsrep_cluster_status')
        if 'Primary' != value:
            logging.error("wsrep cluster status is " + value)
            return False
        
        value = key_value.get('wsrep_connected')
        if 'ON' != value:
            logging.error("wsrep connected is " + value)
            return False
        
        value = key_value.get('wsrep_local_state_comment')
        if value != 'Synced' and value != 'Donor/Desynced':
            logging.error("wsrep local state comment is " + value)
            return False
        
        return True
 
   
    def retrieve_stat_wsrep_status_command(self, conn, key, value, _dict):
        dbsqlstr = 'show status like "wsrep_cluster_status"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_running_day_command(self, conn, key, value, _dict):
        dbsqlstr = 'show global status like "uptime"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_connection_count_command(self, conn, key, value, _dict):
        dbsqlstr = 'show status like "Threads_connected"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)

    def retrieve_stat_active_count_command(self, conn, key, value, _dict):
        dbsqlstr = 'show status like "Threads_running"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)

    def retrieve_stat_wating_count_command(self, conn, key, value, _dict):
        cursor = conn.cursor()
        cursor.execute('select concat("wait_num ",count(command)) from information_schema.PROCESSLIST  where command<>"Sleep" and time >2')
        rows=cursor.fetchall()
        _dict.setdefault(key, rows[0][0].lstrip('wait_num '))

    def retrieve_stat_net_send_command(self, conn, key, value, _dict):
        dbsqlstr = 'show global status like "Bytes_sent"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_net_rev_command(self, conn, key, value, _dict):
        dbsqlstr = 'show global status like "Bytes_received"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)

    def retrieve_stat_QPS_command(self, conn, key, value, _dict):
        dbsqlstr = 'show global status where variable_name in("com_select")'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)

    def retrieve_stat_Com_commit_command(self, conn, key, value, _dict):
        dbsqlstr = 'show global status like "Com_commit"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_Com_rollback(self, conn, key, value, _dict):
        dbsqlstr = 'show global status like "Com_rollback"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)

    def retrieve_stat_slow_query_command(self, conn, key, value, _dict):
        dbsqlstr = 'show global status like "Slow_queries"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_max_conn_command(self, conn, key, value, _dict):
        dbsqlstr = 'show variables like "max_connections"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_max_err_conn_command(self, conn, key, value, _dict):
        dbsqlstr = 'show variables like "max_connect_errors"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_max_open_file_command(self, conn, key, value, _dict):
        dbsqlstr = 'show variables like "open_files_limit"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_opened_file_command(self, conn, key, value, _dict):
        dbsqlstr = 'show status like "Open_files"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_opened_table_cach_command(self, conn, key, value, _dict):
        dbsqlstr = 'show status like "Open_tables"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
        
    def retrieve_stat_table_cach_command(self, conn, key, value, _dict):
        dbsqlstr = 'show variables like "table_open_cache"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
 
    def retrieve_stat_table_cach_noha_command(self, conn, key, value, _dict):
        dbsqlstr = 'show status like "Opened_tables"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_key_buffer_size_command(self, conn, key, value, _dict):
        dbsqlstr = 'show variables like "key_buffer_size"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_sort_buffer_size_command(self, conn, key, value, _dict):
        dbsqlstr = 'show variables like "sort_buffer_size"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_join_buffer_size_command(self, conn, key, value, _dict):
        dbsqlstr = 'show variables like "join_buffer_size"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_key_blocks_unused_command(self, conn, key, value, _dict):
        dbsqlstr = 'show status like "Key_blocks_unused"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_key_blocks_used_command(self, conn, key, value, _dict):
        dbsqlstr = 'show status like "key_blocks_used"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_key_blocks_not_flushed_command(self, conn, key, value, _dict):
        dbsqlstr = 'show status like "key_blocks_not_flushed"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_key_buffer_reads_command(self, conn, key, value, _dict):
        dbsqlstr = 'show status like "key_reads"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_key_buffer_reads_request_command(self, conn, key, value, _dict):
        dbsqlstr = 'show status like "key_read_requests"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_key_buffer_writes_command(self, conn, key, value, _dict):
        dbsqlstr = 'show status like "key_writes"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_key_buffer_writes_request_command(self, conn, key, value, _dict):
        dbsqlstr = 'show status like "key_write_requests"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_innodb_bufferpool_size_command(self, conn, key, value, _dict):
        dbsqlstr = 'show variables like "innodb_buffer_pool_size"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_innodb_bufferpool_reads_command(self, conn, key, value, _dict):
        dbsqlstr = 'show status like "innodb_buffer_pool_reads"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_innodb_bufferpool_read_request_command(self, conn, key, value, _dict):
        dbsqlstr = 'show status like "innodb_buffer_pool_read_requests"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_table_space_analyze_command(self, conn, key, value, _dict):
        cursor = conn.cursor()
        cursor.execute("set names 'utf8'")
        cursor.execute('select table_name, table_comment, (data_length+index_length)/1024 as total_kb from information_schema.tables where table_schema="{0}"'.format(value))
        rows=cursor.fetchall()
        row_dict = {}
        if rows == ():
            raise UserVisiableException('%s param given is wrong!' % str(key+'='+value))
        for row in rows:
            __dict = {}
            __dict.setdefault('table_comment', row[1])
            __dict.setdefault('total_kb', str(row[2]))
            row_dict.setdefault(row[0], __dict)      
        _dict.setdefault(key, row_dict)
    
    def retrieve_stat_wsrep_local_fail_command(self, conn, key, value, _dict):
        dbsqlstr = 'show status like "wsrep_local_cert_failures"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_wsrep_local_bf_aborts_command(self, conn, key, value, _dict):
        dbsqlstr = 'show status like "wsrep_local_bf_aborts"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_wsrep_local_replays_command(self, conn, key, value, _dict):
        dbsqlstr = 'show status like "wsrep_local_replays"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_wsrep_replicated_command(self, conn, key, value, _dict):
        dbsqlstr = 'show status like "wsrep_replicated"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_wsrep_replicated_bytes_command(self, conn, key, value, _dict):
        dbsqlstr = 'show status like "wsrep_replicated_bytes"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_wsrep_received_command(self, conn, key, value, _dict):
        dbsqlstr = 'show status like "wsrep_received"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_wsrep_received_bytes_command(self, conn, key, value, _dict):
        dbsqlstr = 'show status like "wsrep_received_bytes"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_wsrep_flow_control_paused_command(self, conn, key, value, _dict):
        dbsqlstr = 'show status like "wsrep_flow_control_paused"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_wsrep_flow_control_sent_command(self, conn, key, value, _dict):
        dbsqlstr = 'show status like "wsrep_flow_control_sent"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_wsrep_flow_control_recv_command(self, conn, key, value, _dict):
        dbsqlstr = 'show status like "wsrep_flow_control_recv"'
        self.base_db_fecth_info(conn, key, dbsqlstr, _dict)
    
    def retrieve_stat_database_size_command(self, conn, key, value, _dict):
        cursor = conn.cursor()
        cursor.execute('select (sum(DATA_LENGTH)+sum(INDEX_LENGTH))/1024 FROM information_schema.TABLES where TABLE_SCHEMA="{0}"'.format(value))
        rows=cursor.fetchall()
        if rows[0][0] == None:
            raise UserVisiableException('%s param given is wrong!' % key)
        _dict.setdefault(key, str(rows[0][0]))
    
    def retrieve_stat_version_command(self, conn, key, value, _dict):
        cursor = conn.cursor()
        cursor.execute('select version()')
        rows=cursor.fetchall()
        _dict.setdefault(key, rows[0][0])
    
    @run_on_executor() 
    @run_callback
    def retrieve_node_info_stat(self, params):
        if not params:
            raise UserVisiableException('params are not given')
        _dict = {}
        conn=self.get_mysql_connection()
        if conn==None:
            raise UserVisiableException("Can\'t connect to mysql server")
        try:            
            for item in params:
                getattr(self, 'retrieve_' + item)(conn, item, params[item], _dict)
            return _dict
        except AttributeError:
            raise UserVisiableException('%s param given is wrong!' % str(item))    
        finally:
            conn.close()
        
    def base_db_fecth_info(self, conn, key, dbsqlstr, _dict):
        cursor = conn.cursor()
        cursor.execute(dbsqlstr)
        rows=cursor.fetchall()
        _dict.setdefault(key, rows[0][1])

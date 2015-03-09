'''
Created on 2013-7-21

@author: asus
'''
import MySQLdb
import logging

from tornado.options import options
from common.helper import retrieve_kv_from_db_rows
from common.utils.exceptions import CommonException

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
            sql = """drop user {username}@'{ip_address}'""".format(username=username,ip_address=ip_address)
            logging.info("drop user sql is:" + sql)
            cursor.execute(sql)
    
    def check_create_table(self, conn, tb_name , db_name):
        conn.select_db(db_name)
        cursor = conn.cursor()
        try:
            sql = "select * from %s" % (tb_name)
            cursor.execute(sql)
        except MySQLdb.Error, e:
            sql = "CREATE TABLE if not exists %s ( `id` int(12) NOT NULL AUTO_INCREMENT ,`time` varchar(32) NOT NULL, `identifier` varchar(64) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8" % (tb_name)
            cursor.execute(sql)
        logging.info('create table ' + tb_name + ' success')

    def insert_record_time(self, conn, time_data, identifier, tb_name, db_name):
        conn.select_db(db_name)
        cursor = conn.cursor()
        sql = "INSERT INTO %s (time, identifier ) VALUES('%s','%s')" % (tb_name, time_data, identifier)
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
            sql = "SELECT count(*) from %s" % (tb_name)
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
            sql = "truncate %s" % (tb_name)
            cursor.execute(sql)
        except Exception, e:
            logging.exception(e)
        
    def query_record_time(self, conn, identifier, tb_name, db_name):
        conn.select_db(db_name)
        cursor = conn.cursor()
        try:
            sql = "select time from %s where identifier = '%s' order by id desc limit 1" %(tb_name, identifier)
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
            sql = "select count(*) from %s" % (tb_name)
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
            cursor.execute("""grant usage on {dbName}.* to {username}@'{ip_address}' identified by '{passwd}'"""
                           .format(dbName=dbName,username=username,passwd=passwd,ip_address=ip_address))
        else:
            cursor.execute("""CREATE USER {username}@'{ip_address}' IDENTIFIED BY '{passwd}'"""
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
                                usage on {database}.* to {username}@'{ip_address}' with 
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
        cursor.execute("create database if not exists " + databaseName)
        
    def drop_database(self, conn, databaseName):
        cursor = conn.cursor()
        cursor.execute("drop database if exists " + databaseName)
        
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
        cursor.execute("show procedure status")
        rows = cursor.fetchall()
        logging.info(str(rows))
        
        c = len(rows)
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
    
    def get_mysql_connection(self, host = None, autocommit = True):
        conn = None
        
        try:
            conn=MySQLdb.Connect(host=options.mysql_host,user='root',passwd='Mcluster',port=options.mysql_port)
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
        

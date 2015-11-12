import sys, os
import time
import urllib
import httplib
import json
from connsync import Connsync
from mail import send_email
import logging
import logging.config
from reset_master_config import MASTER_USER, MASTER_PASSWORD, MASTER_ROOT_USER, MASTER_ROOT_USER_PASSWORD, ADMIN_MAIL

# Immutable string
BIN_LOG_PATH = "/db/binlog/pos"
NODE_STAT_PATH = "/db/binlog/node/stat"

class Replication(Connsync):
    __slots__ = ('data', 'status', 'current_master', 'started_nodes', 'masters')

    def __init__(self, status):
        self.data = {}
        self.status = status
        self.current_master = ''
        self.started_nodes =[]
        self.masters = []

    def check_slave_status(self, conn):
        sql_show_slave = 'show slave status'
        rows_show_slave = self.exc_mysql_sql(conn, sql_show_slave)
        self.current_master = rows_show_slave[0][1]
        
        self.__get_started_nodes()
        self.__get_master_nodes()
        
        logging.info("status: %s" % rows_show_slave[0][10])
        
        if 'Yes' != rows_show_slave[0][10] or 'Yes' != rows_show_slave[0][11]:         
            self.data['Relay_Log_Pos'] = rows_show_slave[0][8]
            rows_xid = self.exc_mysql_sql(conn, "SHOW RELAYLOG EVENTS IN '%s'" %rows_show_slave[0][7])
            self.data['xid'] = long(rows_xid[-1][-1].strip('COMMIT /* xid='))
            logging.info(self.data)
            return False
        return True
    
    def __get_master_nodes(self):
        if not self.masters:
            self.masters.append(self.current_master)
            for started_node in self.started_nodes:
                code, response_data = self.http_request(started_node, NODE_STAT_PATH)
                while 200 != code:
                    time.sleep(3)
                    code, response_data = self.http_request(started_node, NODE_STAT_PATH)
                if 'ON' == json.loads(response_data)['response']['stat_log_bin']:
                    self.masters.append(started_node)
            logging.info(self.masters) 
    
    def __get_started_nodes(self):
        if not self.started_nodes:
            code, response_data = self.http_request(self.current_master, NODE_STAT_PATH)
            while 200 != code:
                time.sleep(3)
                code, response_data = self.http_request(self.current_master, NODE_STAT_PATH)
            logging.info(json.loads(response_data))
            self.started_nodes = json.loads(response_data)['response']['node_list']


    def __get_another_master_binlogpos(self):
        self.current_master = self.__select_other_master()
        logging.info("change master to %s" %self.current_master)
        params = {"xid":self.data['xid']}
        
        code, response_data = self.http_request(self.current_master, BIN_LOG_PATH, params)
        while 200 != code:
            time.sleep(3)
            code, response_data = self.http_request(self.current_master, BIN_LOG_PATH, params)

        logging.info(json.loads(response_data))
        self.data['End_log_pos'] = long(json.loads(response_data)['response']['End_Log_Pos'])
        self.data['Master_Log_File'] = json.loads(response_data)['response']['Master_Log_File']

    def http_request(self, host='127.0.0.1', path='/', params={}):
        params = urllib.urlencode(params)
        headers = {"Content-type": "application/x-www-form-urlencoded"
                    , "Accept": "text/plain"}
        conn = httplib.HTTPConnection(host,'8888')
        conn.request('POST', path, params, headers)
        response = conn.getresponse()
        data = response.read()
        conn.close()
        return response.status, data

    def __reset_mysql_master(self, conn):
        sqlstr = '''CHANGE MASTER TO MASTER_HOST="{0}",
                        MASTER_PORT=3306,
                        MASTER_USER="{1}",
                        MASTER_PASSWORD="{2}",
                        MASTER_LOG_FILE="{3}",
                        MASTER_LOG_POS={4};'''.format(
                        self.current_master, MASTER_USER, MASTER_PASSWORD,
                        self.data['Master_Log_File'],
                        self.data['End_log_pos'])
        try:
            self.check_relaylog_ready(conn)
            self.exc_mysql_sql(conn,'stop slave')
            self.exc_mysql_sql(conn, sqlstr)
            self.exc_mysql_sql(conn,'start slave')
        except Exception,e:
            logging.info(e)

    def check_relaylog_ready(self, conn):
        rows = self.exc_mysql_sql(conn,'show slave status')
        while self.data['Relay_Log_Pos'] != long(rows[0][8]):
            self.data['Relay_Log_Pos'] = long(rows[0][8])
            time.sleep(2)
            rows = self.exc_mysql_sql(conn,'show slave status')
            #print self.data['Relay_Log_Pos']

    def __select_other_master(self):
        if len(self.masters) < 2:
            logging.info('other node not open bin-log!')
            raise  ValueError('other node not open bin-log!')
        return [ip for ip in self.masters if self.current_master != ip][0]

    def _send_email(self, data_node_ip, text):
        try:
            subject = "[%s] %s" % (data_node_ip, text)
            body = "[%s] %s" % (data_node_ip, text)
            send_email(ADMIN_MAIL, subject, body)
        except Exception, e:
            raise e

    def epoch(self,conn):
        self.__get_another_master_binlogpos()
        self.__reset_mysql_master(conn)


def main():
    if os.path.exists(r'/var/log/reset-master/') is False:
        os.mkdir(r'/var/log/reset-master/')
        with open(r'/var/log/reset-master/root.log','a'):
            pass

    logging.config.fileConfig('logging.conf')
    rep = Replication(status=True)
    while rep.status:
        conn = rep.get_mysql_connection('127.0.0.1', MASTER_ROOT_USER, MASTER_ROOT_USER_PASSWORD)
        if conn is None:
            logging.info("connect local mysql is wrong")
        try:
            if rep.check_slave_status(conn) is False:
                rep._send_email(rep.current_master,'mysql master-slave connect is wrong; pelase check it!')
                rep.epoch(conn)
        except Exception,e:
            logging.info(e)
        finally:
            conn.close()
        time.sleep(3)

if __name__=='__main__':
    main()

import sys
import time
import urllib
import httplib
import json
from connsync import Connsync
from mail import send_email

#user config 
LOCAL_SLAVE_IP ='10.154.238.62'
MASTER_HOST = ('10.154.238.133','10.154.238.134')
MASTER_USER = 'rep'
MASTER_PASSWORD = 'gq123'
MASTER_ROOT_USER = 'root'
MASTER_ROOT_USER_PASSWORD = 'Mcluster'


# Immutable string
ADMIN_MAIL = ("xuyanwei <xuyanwei@letv.com>",)
PATH = '/db/all/stat/binlog/end_log_pos'

class Replication(Connsync):

    def __init__(self, status):
        self.data = {}
        self.status = status
        self.current_master = ''

    def check_slave_status(self, conn):
        sql_show_slave = 'show slave status'
        rows_show_slave = self.exc_mysql_sql(conn, sql_show_slave)
        print rows_show_slave[0][10], rows_show_slave[0][11]
        if 'Yes' != rows_show_slave[0][10] or 'Yes' !=rows_show_slave[0][11]:
            self.current_master = rows_show_slave[0][1]
            print self.current_master
            self.data['Relay_Log_Pos'] = rows_show_slave[0][8]
            rows_xid = self.exc_mysql_sql(conn, "SHOW RELAYLOG EVENTS IN '%s'" %rows_show_slave[0][7])
            self.data['xid'] = long(rows_xid[-1][-1].strip('COMMIT /* xid='))
            print self.data
            return False
        return True

    def __get_another_master_binlogpos(self):
        master_ip = self.select_other_master()
        print master_ip
        self.current_master = master_ip
        params = {"xid":self.data['xid']}
        code, response_data = self.http_request(master_ip, PATH, params)
        while code !=200:
            time.sleep(3)
            code, response_data = self.http_request(master_ip, PATH, params)
        print code, json.loads(response_data)
        self.data['End_log_pos'] = json.loads(response_data)['response']['End_Log_Pos']
        self.data['Master_Log_File'] = json.loads(response_data)['response']['Master_Log_File']
        assert self.data

    def http_request(self, host='127.0.0.1', path='/', params={}):
        params = urllib.urlencode(params)
        headers = {"Content-type": "application/x-www-form-urlencoded"
                    , "Accept": "text/plain"}
        conn = httplib.HTTPConnection(host,'8888')
        conn.request('POST', path, params, headers)
        response = conn.getresponse()
        data = response.read()
        print response.status, data
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
            print e

    def check_relaylog_ready(self, conn):
        rows = self.exc_mysql_sql(conn,'show slave status')
        while self.data['Relay_Log_Pos'] != long(rows[0][8]):
            self.data['Relay_Log_Pos'] = long(rows[0][8])
            time.sleep(2)
            rows = self.exc_mysql_sql(conn,'show slave status')
            print self.data['Relay_Log_Pos']

    def select_other_master(self):
        return [ip for ip in MASTER_HOST if self.current_master!=ip][0]

    #return current master
    @property
    def master_binlog(self, conn):
        rows = self.get_mysql_data(conn, 'show master status')
        return rows[-1]

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
    rep = Replication(status=True)
    while rep.status:
        print "---------"
        conn = rep.get_mysql_connection(host ='127.0.0.1', user=MASTER_ROOT_USER, passwd=MASTER_ROOT_USER_PASSWORD)
        if conn is None:
            print "connect local mysql is wrong"

        if rep.check_slave_status(conn) is False:
            rep._send_email(rep.current_master,'mysql master-slave connect is wrong; pelase check it!')
            rep.epoch(conn)

        print "---------"
        conn.close()
        time.sleep(3)

if __name__=='__main__':
    main()

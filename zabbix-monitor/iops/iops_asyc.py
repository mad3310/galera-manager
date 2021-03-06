import sys
import time
import MySQLdb
import urllib
import httplib
import base64
import json
from multiprocessing.pool import ThreadPool
import threading

MATRIX_IP = '10.130.85.173'
MATRIX_USER = 'python_monitor'
MATRIX_PASSWD = 'Passw0rd'

PORT = 3306

DES_STORE_IP = '127.0.0.1'
DES_STORE_USER = 'root'
DES_STORE_PASSWORD = 'Mcluster'
DES_STORE_DATABASE = 'iops_hp'
DES_STORE_DATABASE_TABLE = 'iops_table'

DEL_CONTAINER_NAME = []

TIMEOUT = 30
TIME_SLEEP = 10

HCLUSTER_NAME=['Hp_Mcluster']


class Iops(object):
    def __init__(self, status):
        self.status = False
        self.hcluster_names = []
        self.hosts = {}
        self.hosts_container = {}
        self.lock = threading.Lock()
        self._real_init()

    def _real_init(self):
        conn = self.get_mysql_connection(MATRIX_IP, MATRIX_USER, MATRIX_PASSWD, PORT)
        if conn == None:
            raise 'connecting matrix database is wrong'
        try:
            cluster_names_list = self.exc_sql_return(conn, "SELECT hcluster_name FROM paascloud_test.WEBPORTAL_HCLUSTER where type='RDS'")
            [self.hcluster_names.append(cluster_name[0]) for cluster_name in cluster_names_list]
            self.__get_cluster_hosts_info(conn)
            self.__get_container_name(conn)
        finally:
            conn.close()

    def __new__(cls, status):
        cls.status = status
        return object.__new__(cls)

    def __get_cluster_hosts_info(self, conn):
        hcluster_list = [hcluster for hcluster in HCLUSTER_NAME if hcluster in self.hcluster_names]
        if hcluster_list:
            for hcluster in hcluster_list:
                hosts_list = self.exc_sql_return(conn, "select HOST_NAME, HOST_IP from paascloud_test.WEBPORTAL_HOST, \
                paascloud_test.WEBPORTAL_HCLUSTER where paascloud_test.WEBPORTAL_HOST.HCLUSTER_ID=paascloud_test.WEBPORTAL_HCLUSTER.ID \
                and paascloud_test.WEBPORTAL_HCLUSTER.HCLUSTER_NAME='{0}'".format(hcluster))
                if hosts_list:
                    [ self.hosts.setdefault(_host[0], _host[1]) for _host in hosts_list ]

    def __get_container_name(self, conn):
        if self.hosts:
            for host in self.hosts:
                __dict = {}
                _ip = self.hosts[host]
                __dict[host] = _ip
                container_name_list = self.exc_sql_return(conn, "SELECT container_name FROM paascloud_test.WEBPORTAL_CONTAINER where HOST_IP='{ip}' and status <> 9 and type <> 'mclustervip'".format(ip=_ip))
                [self.hosts_container.setdefault(container_name[0], __dict) for container_name in container_name_list]

    def start(self):
        assert self.hcluster_names
        assert self.hosts
        assert self.hosts_container
        self.status = True
    
    ''' http request '''
    @staticmethod
    def http_respond(httpconn, user=('root','root'), path=[], params={}):
        params = urllib.urlencode(params)
        headers = {"Content-type": "application/x-www-form-urlencoded"
                    , "Accept": "text/plain"
                    , "Authorization" : base64.encodestring("Basic %s:%s" % (user[0], user[1])).strip('\n') }
        method = 'GET'

        if path:
            for _path in path:
                httpconn.request(method, _path, params, headers)
                res = httpconn.getresponse()
                yield res.status, res.read()
    
    ''' Thread methed; not thread safety'''
    def request(self, hostip, path, hostname, conn):
        hostip = hostip
        path = path 
        hostname = hostname
        while Iops.status:
            _begin_time = time.time()
            res_data = []
            print hostname + ': ' + hostip
            try:
                httpconn = httplib.HTTPConnection(host=hostip, port=6666, timeout=TIMEOUT)
                [res_data.append(json.loads(_res_response[1])['response']) for _res_response in Iops.http_respond(httpconn, user=('root','root'), path=path) if _res_response[0] == 200]
            except Exception, e:
                print e
            finally:
                httpconn.close()
            print "res_data count: " + str(len(res_data)) 
            if res_data:
                self._insert_data(hostname=hostname, hostip=hostip, conn=conn, res=res_data)

            _end_time = time.time()
            time_span = round((_end_time-_begin_time), 3)
            print time_span
            
            if time_span < TIME_SLEEP:
                time.sleep(TIME_SLEEP - time_span)


    ''' store data into mysql'''
    def _insert_data(self, hostname='', hostip='127.0.0.1', conn=None, res=[]):
        print threading.current_thread().name
        self.lock.acquire(2)
        for _res in res:
            print _res
            if _res['diskiops']:
                model = Model(_res['containerName'], hostname, hostip, _res['diskiops']['read'], _res['diskiops']['write'], _res['time'])
                model.save(conn)
                #del model
        self.lock.release()

    def linq_to_dict(self):
        _dict = {}
        for k,v in self.hosts_container.items():
            for i in v:
                __dict = {}
                if i in _dict:
                    _dict[i][v[i]].append(k)
                    __dict[v[i]] = _dict[i][v[i]]
                else:
                    __dict[v[i]] = [k]
                _dict[i] = __dict
        self.req_data = _dict
        with open('%s.log'%DES_STORE_DATABASE,'w') as f:
            [f.writelines(_dict_key+': '+str(self.hosts_container[_dict_key])+'\n') for _dict_key in self.hosts_container]


    @staticmethod
    def get_mysql_connection(host, user, passwd, port):
        conn = None
        try:
            conn = MySQLdb.Connect(host=host, user=user, passwd=passwd, port=port)
        except Exception,e:
            raise e
        return conn

    @staticmethod
    def exc_sql_return(conn, sqlstr):
        cursor = conn.cursor()
        cursor.execute(sqlstr)
        rows = cursor.fetchall()
        return rows

''' model class'''
class Model(object):
    __slots__ = ('container_name', 'host_name', 'host_ip', 'read', 'write', 'request_time')

    def __init__(self, container_name='', host_name='', host_ip='', read=0, write=0, request_time=''):
        self.container_name = container_name
        self.host_name = host_name
        self.host_ip = host_ip
        self.read = read
        self.write = write
        self.request_time = request_time

    def __new__(cls, *args):
        return object.__new__(cls)

    def __unicode__(self):
        return u"%s"%(self.container_name)

    class Meta:
        ordering=["write"]

    @property
    def _get_read_val(self):
        return int(self.read)

    @property
    def _get_write_val(self):
        return int(self.write)

    def save(self, conn):
        try:
            cursor = conn.cursor()
            sqlstr_insert = "insert into %s.%s (container_name, host_name, host_ip, `read`, `write`, request_time) \
                      VALUES('%s', '%s', '%s', %s, %s, '%s')" \
                      %(DES_STORE_DATABASE, DES_STORE_DATABASE_TABLE, self.container_name, self.host_name, \
                        self.host_ip, self._get_read_val, self._get_write_val, self.request_time)
            print sqlstr_insert + '\n'
            cursor.execute(sqlstr_insert)
            conn.commit()
        except MySQLdb.Error:
            try:
                sql_create_database = "create database %s" % DES_STORE_DATABASE
                cursor.execute(sql_create_database)
            except Exception,e:
                pass
            finally:
                sql_create_table = "CREATE TABLE if not exists %s ( `id` int(12) NOT NULL AUTO_INCREMENT ,`container_name` \
                                varchar(50) NOT NULL, `host_name` varchar(50) NOT NULL, `host_ip` varchar(32) NOT NULL, \
                                `read` int NOT NULL, `write` int NOT NULL, `request_time` varchar(32) NOT NULL, PRIMARY KEY (id)) \
                                ENGINE=InnoDB DEFAULT CHARSET=utf8" % (DES_STORE_DATABASE + '.' + DES_STORE_DATABASE_TABLE)
                cursor.execute(sql_create_table)
        except Exception,e:
            raise e



def main():
    print "------------------"
    iops = Iops(True)
    iops.start()
    if DEL_CONTAINER_NAME:
        for _del_str in DEL_CONTAINER_NAME:
            del iops.hosts_container[_del_str]
    #print iops.hosts_container
    iops.linq_to_dict()
    print "-------------------"
    conn = iops.get_mysql_connection(DES_STORE_IP, DES_STORE_USER, DES_STORE_PASSWORD, PORT)
    if conn == None:
        raise TypeErro(r'src_store_addr is not connect!')

    pool = ThreadPool(processes=len(iops.req_data))
    pool.daemon=True
    for _info in iops.req_data:
        _path = []
        _path = ['/container/stat/{0}/diskiops'.format(i).encode("utf-8") for i in iops.req_data[_info].values()[0]]
        pool.apply_async(iops.request, (iops.req_data[_info].keys()[0], _path, _info, conn))

    pool.close()
    pool.join()

    if conn is not None:
        conn.close()

if __name__ == '__main__':
    main()

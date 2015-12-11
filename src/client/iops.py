import sys
import time
import MySQLdb
import urllib
import httplib
import base64
import json

MATRIX_IP = '10.130.85.173'
MATRIX_USER = 'python_monitor'
MATRIX_PASSWD = 'Passw0rd'

PORT = 3306

DES_STORE_IP = '127.0.0.1'
DES_STORE_USER = 'root'
DES_STORE_PASSWORD = 'xuyanwei'
DES_STORE_DATABASE = 'iops'
DES_STORE_DATABASE_TABLE = 'iops_table'

HCLUSTER_NAME=['Hp_Mcluster']


class Iops(object):
    def __init__(self):
        self.status = False
        self.hcluster_names = []
        self.hosts = {}
        self.hosts_container = {}
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

    def __get_cluster_hosts_info(self, conn):
        hcluster_list = [hcluster for hcluster in HCLUSTER_NAME if hcluster in self.hcluster_names]
        if hcluster_list:
            for hcluster in hcluster_list:
                hosts_list = self.exc_sql_return(conn, "select HOST_NAME, HOST_IP from paascloud_test.WEBPORTAL_HOST, \
                paascloud_test.WEBPORTAL_HCLUSTER where paascloud_test.WEBPORTAL_HOST.HCLUSTER_ID=paascloud_test.WEBPORTAL_HCLUSTER.ID \
                and paascloud_test.WEBPORTAL_HCLUSTER.HCLUSTER_NAME='{0}'".format(hcluster))
                if hosts_list:
                    [ self.hosts.setdefault(_host[0],_host[1]) for _host in hosts_list ]

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

    def http_response(self, user=(), port=8888, method='GET', host='127.0.0.1', path='/', params={}):
        params = urllib.urlencode(params)
        headers = {"Content-type": "application/x-www-form-urlencoded"
                    , "Accept": "text/plain"}

        if user:
            encode_user = base64.encodestring("%s:%s" % (user[0], user[1]))[:-1]
            auth = "Basic %s" % encode_user
            headers.update({"Authorization": auth})

        conn = httplib.HTTPConnection(host, port, timeout=5)
        conn.request(method, path, params, headers)
        response = conn.getresponse()
        data = response.read()
        conn.close()
        return response.status, data

    def request(self, user, host, path):
        try:
            print path
            code, response_data = self.http_response(user=user, port=6666, host=host, path=path)
            if code == 200:
                return response_data
        except Exception, e:
            return {}

    def _store_data(self, conn, dataclass):
        try:
            cursor = conn.cursor()
            sqlstr_insert = "insert into %s.%s (container_name, host_name, host_ip, `read`, `write`, request_time) \
                      VALUES('%s', '%s', '%s', %s, %s, '%s')" \
                      %(DES_STORE_DATABASE, DES_STORE_DATABASE_TABLE, dataclass.container_name, dataclass.host_name, \
                        dataclass.host_ip, dataclass._get_read_val, dataclass._get_write_val, dataclass.request_time)
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


class Model(object):
    __slots__ = ('container_name', 'host_name', 'host_ip', 'read', 'write', 'request_time')

    def __init__(self, container_name='', host_name='', host_ip='', read=0, write=0, request_time=''):
        self.container_name = container_name
        self.host_name = host_name
        self.host_ip = host_ip
        self.read = read
        self.write = write
        self.request_time = request_time

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


def main():
    print "------------------"
    iops = Iops()
    iops.start()
    print iops.hosts_container
    with open('tmp.log','w') as f:
        [f.writelines(_dict_key+': '+str(iops.hosts_container[_dict_key])+'\n') for _dict_key in iops.hosts_container]

    while iops.status:
        container_ip = ''
        host_name = ''
        path = ''
        conn = iops.get_mysql_connection(DES_STORE_IP, DES_STORE_USER, DES_STORE_PASSWORD, PORT)
        if conn == None:
            raise 'src_store_addr is not connect!'

        for _host_dict in iops.hosts_container:
            path ='/container/stat/{0}/diskiops'.format(_host_dict).encode("utf-8")
            container_info = [(iops.hosts_container[_host_dict][__host], __host) for __host in  iops.hosts_container[_host_dict]]
            response_data = iops.request(('root','root'), container_info[0][0], path)
            if response_data:
                obj = json.loads(response_data)['response']
                model = Model(obj['containerName'], container_info[0][1], container_info[0][0], obj['diskiops']['read'], obj['diskiops']['write'], obj['time'])
                iops._store_data(conn, model)

        if conn is not None:
            conn.close()
        #time.sleep(1)


if __name__ == '__main__':
	main()
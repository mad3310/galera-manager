import unittest
import time
import base64
import requests
from pip._vendor.requests.exceptions import HTTPError


class TestMcluster(unittest.TestCase):
    encode_user = base64.encodestring("%s:%s" % ('root', 'root'))
    auth = "Basic %s" % encode_user
    headers = {
        "Content-type": "application/x-www-form-urlencoded",
        "Accept": "text/plain"}
    headers["Authorization"] = auth

    test_container1_ip = ''
    test_container2_ip = ''
    test_container3_ip = ''

    zookeeper_ips = []

    zkip = None

    def setUp(self):
        if not self.zkip:
            with open('/opt/letv/mcluster-manager/api/config/mclusterManager.cnf', 'r') as f:
                self.zkip = f.readlines()[0].split('=')[1].strip('\n')

    def _makeOne(self, **kw):
        from kazoo.retry import KazooRetry
        return KazooRetry(**kw)

    def test_connection_closed(self):
        from kazoo.exceptions import ConnectionClosedError
        retry = self._makeOne()

        def testit():
            raise ConnectionClosedError()
        self.assertRaises(ConnectionClosedError, retry, testit)

    def test_session_expired(self):
        from kazoo.exceptions import SessionExpiredError
        retry = self._makeOne(max_tries=1)

        def testit():
            raise SessionExpiredError()
        self.assertRaises(Exception, retry, testit)

    def test_admin_conf(self):
        payload = dict(zkAddress=self.zookeeper_ips[0], zkPort='2181')
        r = requests.post('http://%s:8888/admin/conf' %
                          self.test_container1_ip, data=payload)

        def testit():
            raise HTTPError()
        self.assertEqual(200, r.status_code, testit)

    def test_admin_user(self):
        payload = dict(adminUser='root', adminPassword='root')
        r = requests.post('http://%s:8888/admin/user' %
                          self.test_container1_ip, data=payload)

        def testit():
            raise HTTPError()
        self.assertEqual(200, r.status_code, testit)

    def test_cluster(self):
        payload = dict(clusterName='mcluster-manager-test', dataNodeIp=self.test_container1_ip,
                       dataNodeName='d-mcl-mcluster-manager-test-n-1')
        r = requests.post('http://%s:8888/cluster' %
                          self.test_container1_ip, data=payload, headers=self.headers)

        def testit():
            raise HTTPError()
        self.assertEqual(200, r.status_code, testit)

    def test_cluster_init(self):
        r = requests.get('http://%s:8888/cluster/init?forceInit=false' %
                         self.test_container1_ip, headers=self.headers)

        def testit():
            raise HTTPError()
        self.assertEqual(200, r.status_code, testit)

    def test_cluster_db_start(self):
        payload = dict(role='manager', dbName='testdb', userName='test')
        r = requests.post('http://%s:8888/dbUser' %
                          self.test_container1_ip, data=payload, headers=self.headers)

        def testit():
            raise HTTPError()
        self.assertEqual(200, r.status_code, testit)

    def test_admin_conf(self):
        payload = dict(zkAddress=self.zookeeper_ips[1], zkPort='2181')
        r = requests.post('http://%s:8888/admin/conf' %
                          self.test_container2_ip, data=payload)

        def testit():
            raise HTTPError()
        self.assertEqual(200, r.status_code, testit)

    def test_cluster_sync(self):
        r = requests.get('http://%s:8888/cluster/sync' %
                         self.test_container2_ip)

        def testit():
            raise HTTPError()
        self.assertEqual(200, r.status_code, testit)

    def test_cluster_node(self):
        payload = dict(dataNodeIp=self.test_container2_ip,
                       dataNodeName='d-mcl-mcluster-manager-test-n-2')
        r = requests.post('http://%s:8888/cluster/node' %
                          self.test_container2_ip, data=payload, headers=self.headers)

        def testit():
            raise HTTPError()
        self.assertEqual(200, r.status_code, testit)

    def test_admin_conf(self):
        payload = dict(zkAddress=self.zookeeper_ips[2], zkPort='2181')
        r = requests.post('http://%s:8888/admin/conf' %
                          self.test_container3_ip, data=payload)

        def testit():
            raise HTTPError()
        self.assertEqual(200, r.status_code, testit)

    def test_cluster_sync(self):
        r = requests.get('http://%s:8888/cluster/sync' %
                         self.test_container3_ip)

        def testit():
            raise HTTPError()
        self.assertEqual(200, r.status_code, testit)

    def test_cluster_node(self):
        payload = dict(dataNodeIp=self.test_container3_ip,
                       dataNodeName='d-mcl-mcluster-manager-test-n-3')
        r = requests.post('http://%s:8888/cluster/node' %
                          self.test_container3_ip, data=payload, headers=self.headers)

        def testit():
            raise HTTPError()
        self.assertEqual(200, r.status_code, testit)

    def test_cluster_start(self):
        payload = dict(cluster_flag='new')
        r = requests.post('http://%s:8888/cluster/start' %
                          self.test_container1_ip, data=payload, headers=self.headers)

        def testit():
            raise HTTPError()
        self.assertEqual(200, r.status_code, testit)

    def test_cluster_check(self):
        r = requests.get('http://%s:8888/cluster/check/online_node' %
                         self.test_container1_ip, headers=self.headers)

        def testit():
            time.sleep(2)
            self.test_cluster_check()
        try:
            self.assertEqual(
                3, len(eval(r.text)['response']["nodelist"]), 'testit')
        except AssertionError, e:
            eval(str(e))()

    def tearDown(self):
        print "call this method is finish"

if __name__ == "__main__":
    unittest.main()

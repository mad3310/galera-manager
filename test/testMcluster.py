import unittest

import requests
from pip._vendor.requests.exceptions import HTTPError

class TestMcluster(unittest.TestCase):
    
    def setUp(self):
        with open('/tmp/zkip', 'r') as f:
            self.zkip = f.readline().strip('\n').split('=')[1]
    
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
        payload = dict(zkAddress=self.zkip, zkPort='2181')
        r = requests.get('https://127.0.0.1:8888/admin/conf', data=payload)
        
        def testit():
            raise HTTPError()
        self.assertEqual(200, r.status_code, testit)
        
    def test_admin_user(self):
        payload = dict(adminUser='root', adminPassword='root')
        r = requests.get('https://127.0.0.1:8888/admin/user', data=payload)
        
        def testit():
            raise HTTPError()
        self.assertEqual(200, r.status_code, testit)
        
    def test_cluster(self):
        payload = dict(clusterName='mcluster-manager-test', dataNodeIp='', dataNodeName='d-mcl-mcluster-manager-test-n-1')
        r = requests.post('https://127.0.0.1:8888/cluster', data=payload)
        
        def testit():
            raise HTTPError()
        self.assertEqual(200, r.status_code, testit)
        
    def test_cluster_init(self):
        payload = dict(forceInit=false)
        r = requests.get('https://127.0.0.1:8888/cluster/init', data=payload)
        
        def testit():
            raise HTTPError()
        self.assertEqual(200, r.status_code, testit)      
        
    def test_cluster_sync(self):
        r = requests.get('https://127.0.0.1:8888/cluster/sync')
        
        def testit():
            raise HTTPError()
        self.assertEqual(200, r.status_code, testit)
        
    def test_cluster_node(self):
        payload = dict(dataNodeIp='', dataNodeName='d-mcl-mcluster-manager-test-n-2')
        r = requests.post('https://127.0.0.1:8888/cluster/node', data=payload)
        
        def testit():
            raise HTTPError()
        self.assertEqual(200, r.status_code, testit)
        
    def test_cluster_sync(self):
        r = requests.get('https://127.0.0.1:8888/cluster/sync')

        def testit():
            raise HTTPError()
        self.assertEqual(200, r.status_code, testit)
        
    def test_cluster_node(self):
        payload = dict(dataNodeIp='', dataNodeName='d-mcl-mcluster-manager-test-n-3')
        r = requests.post('https://127.0.0.1:8888/cluster/node', data=payload)
        
        def testit():
            raise HTTPError()
        self.assertEqual(200, r.status_code, testit)
        
    def test_cluster_start(self):
        payload = dict(cluster_flag='new')
        r = requests.post('https://127.0.0.1:8888/cluster/start', data=payload)
        
        def testit():
            raise HTTPError()
        self.assertEqual(200, r.status_code, testit)

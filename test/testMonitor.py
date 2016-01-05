import unittest

import requests
from pip._vendor.requests.exceptions import HTTPError

class TestMonitor(unittest.TestCase):
    
    def setUp(self):
        with open('/tmp/zkip', 'r') as f:
            self.zkip = f.readline().split('=')[1]
        
    def test_mcluster_monitor(self):
        r = requests.get('https://127.0.0.1:8888/mcluster/monitor')

        def testit():
            raise HTTPError()
        self.assertEqual(200, r.status_code, testit)
        
    def test_monitor_async(self):
        r = requests.get('https://127.0.0.1:8888/monitor/async')
        
        def testit():
            raise HTTPError()
        self.assertEqual(200, r.status_code, testit)
        
    def test_mcluster_status(self):
        r = requests.get('https://127.0.0.1:8888/mcluster/status')
        
        def testit():
            raise HTTPError()
        self.assertEqual(200, r.status_code, testit)
        self.assertEqual('nothing', r.text["node"]['log_health']['alarm'], testit)
        self.assertEqual('nothing', r.text["node"]['log_error']['alarm'], testit)
        
        self.assertEqual('nothing', r.text["db"]['existed_db_anti_item']['alarm'], testit)
        
        self.assertEqual('nothing', r.text["db"]['cur_conns']['alarm'], testit)
        self.assertEqual('nothing', r.text["db"]['wsrep_status']['alarm'], testit)

        self.assertEqual('nothing', r.text["db"]['cur_user_conns']['alarm'], testit)
        self.assertEqual('nothing', r.text["db"]['write_read_avaliable']['alarm'], testit)
        
    def test_db_check_wr(self):
        r = requests.get('https://127.0.0.1:8888/db/check/wr')
        
        def testit():
            raise HTTPError()
        self.assertTrue(r.text, testit)
        
    def test_db_check_wsrep(self):
        r = requests.get('https://127.0.0.1:8888/db/check/wsrep_status')
        
        def testit():
            raise HTTPError()
        self.assertTrue(r.text, testit)
        
    def test_db_check_cur_conns(self):
        r = requests.get('https://127.0.0.1:8888/db/check/cur_conns')
        
        def testit():
            raise HTTPError()
        self.assertTrue(r.text, testit)
        
    def test_db_check_cur_user_conns(self):
        r = requests.get('https://127.0.0.1:8888/db/check/cur_user_conns')
        
        def testit():
            raise HTTPError()
        self.assertTrue(r.text, testit)
        
    def test_node_check_log_error(self):
        r = requests.get('https://127.0.0.1:8888/node/check/log/error')
        
        def testit():
            raise HTTPError()
        self.assertTrue(r.text, testit)
        
    def test_node_check_log_warning(self):
        r = requests.get('https://127.0.0.1:8888/node/check/log/warning')
        
        def testit():
            raise HTTPError()
        self.assertTrue(r.text, testit)
        
    def test_node_check_log_health(self):
        r = requests.get('https://127.0.0.1:8888/node/check/log/health')
        
        def testit():
            raise HTTPError()
        self.assertTrue(r.text, testit) 
    
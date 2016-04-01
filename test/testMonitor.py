import unittest

import requests
from pip._vendor.requests.exceptions import HTTPError

class TestMonitor(unittest.TestCase):
    
    test_container_ip='127.0.0.1'

    def setUp(self):
        pass
        
    def test_mcluster_monitor(self):
        r = requests.get('http://%s:8888/mcluster/monitor' %self.test_container_ip)

        def testit():
            raise HTTPError()
        self.assertEqual(200, r.status_code, testit)
        
    def test_monitor_async(self):
        r = requests.get('http://%s:8888/mcluster/monitor/async' %self.test_container_ip)
        
        def testit():
            raise HTTPError()
        self.assertEqual(200, r.status_code, testit)
        
    def test_mcluster_status(self):
        r = requests.get('http://%s:8888/mcluster/status' %self.test_container_ip)
        
        def testit():
            raise HTTPError()
        self.assertEqual(200, r.status_code, testit)
        self.assertEqual('nothing', eval(r.text)['response']["node"]['log_health']['alarm'], testit)
        self.assertEqual('nothing', eval(r.text)['response']["node"]['log_error']['alarm'], testit)
        
        self.assertEqual('nothing', eval(r.text)['response']["db"]['existed_db_anti_item']['alarm'], testit)
        
        self.assertEqual('nothing', eval(r.text)['response']["db"]['cur_conns']['alarm'], testit)
        self.assertEqual('nothing', eval(r.text)['response']["db"]['wsrep_status']['alarm'], testit)

        self.assertEqual('nothing', eval(r.text)['response']["db"]['cur_user_conns']['alarm'], testit)
        self.assertEqual('nothing', eval(r.text)['response']["db"]['write_read_avaliable']['alarm'], testit)
        
    def test_db_check_wr(self):
        r = requests.get('http://%s:8888/db/check/wr' %self.test_container_ip)
        
        def testit():
            raise HTTPError()
        self.assertTrue(r.text, testit)
        
    def test_db_check_wsrep(self):
        r = requests.get('http://%s:8888/db/check/wsrep_status' %self.test_container_ip)
        
        def testit():
            raise HTTPError()
        self.assertTrue(r.text, testit)
        
    def test_db_check_cur_conns(self):
        r = requests.get('http://%s:8888/db/check/cur_conns' %self.test_container_ip)
        
        def testit():
            raise HTTPError()
        self.assertTrue(r.text, testit)
        
    def test_db_check_cur_user_conns(self):
        r = requests.get('http://%s:8888/db/check/cur_user_conns' %self.test_container_ip)
        
        def testit():
            raise HTTPError()
        self.assertTrue(r.text, testit)
        
    def test_node_check_log_error(self):
        r = requests.get('http://%s:8888/node/check/log/error' %self.test_container_ip)
        
        def testit():
            raise HTTPError()
        self.assertTrue(r.text, testit)
        
    def test_node_check_log_warning(self):
        r = requests.get('http://%s:8888/node/check/log/warning' %self.test_container_ip)
        
        def testit():
            raise HTTPError()
        self.assertTrue(r.text, testit)
        
    def test_node_check_log_health(self):
        r = requests.get('http://%s:8888/node/check/log/health' %self.test_container_ip)
        
        def testit():
            raise HTTPError()
        self.assertTrue(r.text, testit) 
        
if __name__=="__main__":
    unittest.main()


    
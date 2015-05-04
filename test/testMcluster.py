import unittest

import requests

class TestMcluster(unittest.TestCase):
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
        requests.
        
    docker run -i -t --privileged -h manager-zk-3 --name manager-zk-3 letv:centos6 bin/bash
# -*- coding: utf-8 -*-

import unittest
import base64
import requests
import json


class TestBinlog(unittest.TestCase):
    encode_user = base64.encodestring("%s:%s" % ('root', 'root'))
    auth = "Basic %s" % encode_user
    headers = {"Content-type": "application/x-www-form-urlencoded",
               "Accept": "text/plain"}
    headers["Authorization"] = auth
    container_ip = ''

    def test_db_binlog_node_stat(self):
        r = requests.get('http://%s:8888/db/binlog/node/stat' % self.container_ip)
        print r.text
        p = json.loads(r.text)
        code = p["meta"]["code"]
        self.assertEqual(200, code, "dbBinLogNodeState gets an error!")

    def test_db_user(self):
        print "/dbUser"
        payload = dict(role='manager', dbName='testdb', userName='test')
        r = requests.post('http://%s:8888/dbUser' % self.container_ip,
                          data=payload, headers=self.headers)
        print r.text
        p = json.loads(r.text)
        code = p["meta"]["code"]
        self.assertEqual(200, code, " DBUser gets an error!")

if __name__ == "__main__":
    unittest.main()

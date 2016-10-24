# -*- coding: utf-8 -*-

import unittest
import time
import base64
import requests

ipAddrr = ''


class TestDilatation(unittest.TestCase):
    encode_user = base64.encodestring("%s:%s" % ('root', 'root'))
    auth = "Basic %s" % encode_user
    headers = {
        "Content-type": "application/x-www-form-urlencoded",
        "Accept": "text/plain"}
    headers["Authorization"] = auth.strip()

    encode_user1 = base64.encodestring("%s:%s" % ('root', '10_sww_test00003'))
    auth1 = "Basic %s" % encode_user1
    headers1 = {
        "Content-type": "application/x-www-form-urlencoded",
        "Accept": "text/plain"}
    headers1["Authorization"] = auth1.strip()
    clusterName = '10_sww_test00003'
    nodeName = 'd-mcl-10_sww_test00003-n-5'

    def setUp(self):
        pass

    def compare(self, r):
        self.assertEqual(200, eval(r.text)["meta"]["code"])

    def test_step1(self):
        payload = dict(image='10.160.140.32:5002/letv-mcluster:v0.1.9', nodeCount='1',
                       networkMode='ip', componentType='mcluster',
                       containerClusterName=self.clusterName)
        r = requests.post('http://10.154.156.129:8888/containerCluster/node',
                          data=payload, headers=self.headers)
        print 'step1:' + r.text
        self.compare(r)
        time.sleep(50)

    def test_step2(self):
        r = requests.get('http://10.154.156.129:8888/containerCluster/%s/node/%s' %
                         (self.clusterName, self.nodeName), headers=self.headers)
        print 'step2:' + r.text
        global ipAddrr
        ipAddrr = eval(r.text)['response']['containers'][0]['ipAddr']
        print ipAddrr

        self.compare(r)
        time.sleep(30)

    def test_step3(self):
        payload = dict(zkAddress='10.154.255.55', zkPort='2181')
        global ipAddrr
        r = requests.post('http://%s:8888/admin/conf' %
                          ipAddrr, data=payload, headers=self.headers1)
        print "step3: " + r.text
        self.compare(r)
        time.sleep(10)

    def test_step4(self):
        payload = dict(adminUser='root', adminPassword=self.clusterName)
        global ipAddrr
        r = requests.post('http://%s:8888/admin/user' %
                          ipAddrr, data=payload, headers=self.headers1)
        print "step4: " + r.text
        self.compare(r)
        time.sleep(10)

    def test_step5(self):
        payload = dict(dataNodeIp=ipAddrr, dataNodeName=self.nodeName)
        global ipAddrr
        r = requests.post('http://%s:8888/cluster/node' %
                          ipAddrr, data=payload, headers=self.headers1)
        print "step5: " + r.text
        self.compare(r)
        time.sleep(10)

    def test_step6(self):
        payload = dict(isNewCluster='False')
        global ipAddrr
        r = requests.post('http://%s:8888/node/start' %
                          ipAddrr, data=payload, headers=self.headers1)
        print "step6: " + r.text
        self.compare(r)


if __name__ == "__main__":
    unittest.main()

import unittest
import time,base64
import requests
from requests.exceptions import HTTPError
ipAddrr = ''
class TestRemove(unittest.TestCase):
    encode_user = base64.encodestring("%s:%s" % ('root', 'root'))
    auth = "Basic %s" % encode_user
    headers = {
    "Content-type": "application/x-www-form-urlencoded",
    "Accept": "text/plain"}
    headers["Authorization"] = auth.strip()
   
   
    encode_user1 = base64.encodestring("%s:%s" % ('root', '10_sww_test00002'))
    auth1 = "Basic %s" % encode_user1
    headers1 = {
    "Content-type": "application/x-www-form-urlencoded",
    "Accept": "text/plain"}
    headers1["Authorization"] = auth1.strip()  
    
    nodeName='d-mcl-10_sww_test00003-n-5'
    clusterName='10_sww_test00003'
    
    def setUp(self):
        pass
        
    def compare(self,r):
        self.assertEqual(200, eval(r.text)["meta"]["code"])
           
    def test_step1(self):
        r = requests.get('http://10.154.156.129:8888/containerCluster/%s/node/%s' %(self.clusterName,self.nodeName), headers=self.headers)
        print 'step1:'+r.text
        global ipAddrr 
        ipAddrr = eval(r.text)['response']['containers'][0]['ipAddr']
        print ipAddrr
        self.compare(r)
        
    def test_step2(self):
        payload = dict(dataNodeIp=ipAddrr, dataNodeName=self.nodeName)
        global ipAddrr 
        r = requests.delete('http://%s:8888/cluster/node' %ipAddrr, data=payload, headers=self.headers1)
        print 'step2'+r.text
        
        self.compare(r)
        time.sleep(10)
    
    def test_step3(self):
        payload = dict(containerClusterName=self.clusterName, containerNameList=self.nodeName)
        global ipAddrr 
        r = requests.post('http://10.154.156.129:8888/containerCluster/node/remove', data=payload, headers=self.headers)
        print 'step3'+r.text
        self.compare(r)
        

        
if __name__=="__main__":
    unittest.main()
    
    
    

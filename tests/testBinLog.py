import unittest
import base64
import requests
from requests.exceptions import HTTPError

class TestLogBin(unittest.TestCase):
	encode_user = base64.encodestring("%s:%s" % ('root', 'root'))
	auth = "Basic %s" % encode_user
	headers = {
		"Content-type": "application/x-www-form-urlencoded",
		"Accept": "text/plain"}
	headers["Authorization"] = auth

	test_container_ip=''
	def setUp(self):
		pass 	
	def test_binlog_node_stat(self):
		r = requests.get('http://%s:8888/db/binlog/node/stat' %self.test_container_ip)
		print r.text 
		code=eval(r.text)["meta"]["code"]
		self.assertEqual(200,code,"api BinLogPos gets an error!")
	def test_dbUser(self):
		print "/dbUser"
		payload = dict(role='manager', dbName='testdb',userName='test')
		r = requests.post('http://%s:8888/dbUser' %self.test_container_ip, data=payload,headers=self.headers)
		print r.text
		code=eval(r.text)["meta"]["code"]
		self.assertEqual(200,code,"api DBUser gets an error!")       
 	def tearDown(self):
		print "call this method is finish\n"
        
if __name__=="__main__":
	unittest.main()

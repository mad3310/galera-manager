import unittest
import base64
import requests
from requests.exceptions import HTTPError
import json

class TestLogBin(unittest.TestCase):
	encode_user = base64.encodestring("%s:%s" % ('root', 'root'))
	auth = "Basic %s" % encode_user
	headers = {
		"Content-type": "application/x-www-form-urlencoded",
		"Accept": "text/plain"}
	headers["Authorization"] = auth

	container_ip=''
	
	def setUp(self):
		pass 	
	def test_db_binlog_node_stat(self):
		r = requests.get('http://%s:8888/db/binlog/node/stat' %self.container_ip)
		print r.text 
		p=json.loads(r.text)
		code=p["meta"]["code"]
		self.assertEqual(200,code,"dbBinLogNodeState gets an error!")
	def test_dbUser(self):
		print "/dbUser"
		payload = dict(role='manager', dbName='testdb',userName='test')
		r = requests.post('http://%s:8888/dbUser' %self.container_ip, data=payload,headers=self.headers)
		print r.text
		p=json.loads(r.text)
		code=p["meta"]["code"]
		self.assertEqual(200,code," DBUser gets an error!")       
 	def tearDown(self):
		print "call this method is finish\n"
        
if __name__=="__main__":
	unittest.main()

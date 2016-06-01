import unittest
import requests
from requests.exceptions import HTTPError
import json
def get_err_msg(api_type):
	msg={
		"DBStat":" DBStat gets an error!",
		"StatRowsOperTotal":" StatRowsOperTotal gets an error!",
		"StatRowsOperPs":" StatRowsOperPs gets an error!",
		"StatInnoBufferMemallco":" StatInnoBufferMemallco gets an error!",
		"StatInnoBufferPage":" StatInnoBufferPage gets an error!",
		"StatInnoBufferPool":" StatInnoBufferPool gets an error!",
		"StatVariableStatusPs":" StatVariableStatusPs gets an error!",
		"StatVariableStatusUsed":" StatVariableStatusUsed gets an error!",
		"StatVariableStatusRation": " StatVariableStatusRation gets an error!",
		"StatWsrepstatusFlowControlPaused":"StatWsrepstatusFlowControlPaused gets an error!",
		"StatWsrepStatusSlowestNodeParam":" StatWsrepStatusSlowestNodeParam gets an error!",
		"StatWsrepStatusSlowestNetworkParam":" StatWsrepStatusSlowestNetworkParam gets an error!"
		}
	return msg.get(api_type, "unknown api type!")
class TestDB(unittest.TestCase):

	container_ip=""	
	
	def setUp(self):
		pass		
	def test_db_all_stat(self):		
		print "DBStat: "        
		r = requests.get('http://%s:8888/db/all/stat'%self.container_ip)
		print r.text		
		p=json.loads(r.text)
		code=p["meta"]["code"]
		self.assertEqual(200,code,get_err_msg("DBStat"))    
	def test_db_all_stat_rowsoper_total(self):
		print "StatRowsOperTotal: "        
		r = requests.get('http://%s:8888/db/all/stat/rowsoper/total'%self.container_ip)
		print r.text 
		p=json.loads(r.text)
		code=p["meta"]["code"]
		self.assertEqual(200,code,get_err_msg("StatRowsOperTotal"))				
	def test_db_all_stat_rowsoper_ps(self):		
		print "StatRowsOperPs: "  
		r = requests.get('http://%s:8888/db/all/stat/rowsoper/ps'%self.container_ip)
		print r.text
		p=json.loads(r.text)
		code=p["meta"]["code"]
		self.assertEqual(200,code,get_err_msg("StatRowsOperPs"))		
	def test_db_all_stat_innobuffer_memallco(self):		
		print "StatInnoBufferMemallco: "        
		r = requests.get('http://%s:8888/db/all/stat/innobuffer/memallco'%self.container_ip)
		print r.text
		p=json.loads(r.text)
		code=p["meta"]["code"]
		self.assertEqual(200, code, get_err_msg("StatInnoBufferMemallco"))		
	def test_db_all_stat_innobuffer_page(self):		
		print "StatInnoBufferPage: "        
		r = requests.get('http://%s:8888/db/all/stat/innobuffer/page'%self.container_ip)
		print r.text
		p=json.loads(r.text)
		code=p["meta"]["code"]
		self.assertEqual(200,code,get_err_msg("StatInnoBufferPage"))	
	def test_db_all_stat_innobuffer_pool(self):		
		print "StatInnoBufferPool: "        
		r = requests.get('http://%s:8888/db/all/stat/innobuffer/pool'%self.container_ip)
		print r.text
		p=json.loads(r.text)
		code=p["meta"]["code"]
		self.assertEqual(200,code,get_err_msg("StatInnoBufferPool"))	
	def test_db_all_stat_variablestatus_ps(self):		
		print "StatVariableStatusPs: "        
		r = requests.get('http://%s:8888/db/all/stat/variablestatus/ps'%self.container_ip)
		print r.text
		p=json.loads(r.text)
		code=p["meta"]["code"]
		self.assertEqual(200,code,get_err_msg("StatVariableStatusPs"))
		
	def test_db_all_stat_variablestatus_used(self):		
		print "StatVariableStatusUsed: "        
		r = requests.get('http://%s:8888/db/all/stat/variablestatus/used'%self.container_ip)
		print r.text
		p=json.loads(r.text)
		code=p["meta"]["code"]
		self.assertEqual(200,code,get_err_msg("StatVariableStatusUsed"))	
	def test_db_all_stat_variablestatus_ration(self):	
		print "StatVariableStatusRation: "     
		r = requests.get('http://%s:8888/db/all/stat/variablestatus/ration'%self.container_ip)
		print r.text
		p=json.loads(r.text)
		code=p["meta"]["code"]
		self.assertEqual(200,code,get_err_msg("StatVariableStatusRation"))	
	def test_db_all_stat_wsrepstatus_flow_control_paused(self):		
		print "StatWsrepstatusFlowControlPaused: "        
		r = requests.get('http://%s:8888/db/all/stat/wsrepstatus/flow_control_paused'%self.container_ip)
		print r.text
		p=json.loads(r.text)
		code=p["meta"]["code"]
		self.assertEqual(200,code,get_err_msg("StatWsrepstatusFlowControlPaused"))		
	def test_db_all_stat_wsrepstatus_slowest_node_param(self):		
		print "StatWsrepStatusSlowestNodeParam: " 
		r = requests.get('http://%s:8888/db/all/stat/wsrepstatus/slowest_node_param'%self.container_ip)
		print r.text
		p=json.loads(r.text)
		code=p["meta"]["code"]
		self.assertEqual(200,code,get_err_msg("StatWsrepStatusSlowestNodeParam"))	
	def test_db_all_stat_wsrepstatus_slowest_network_param(self):		
		print "StatWsrepStatusSlowestNetworkParam: "        
		r = requests.get('http://%s:8888/db/all/stat/wsrepstatus/slowest_network_param'%self.container_ip)
		print r.text
		p=json.loads(r.text)
		code=p["meta"]["code"]
		self.assertEqual(200,code,get_err_msg("StatWsrepStatusSlowestNetworkParam"))	
	def tearDown(self):
		print "call this method is finish\n"
        
if __name__=="__main__":
	unittest.main()

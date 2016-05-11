import unittest
import requests
from requests.exceptions import HTTPError
null=""
def get_err_msg(api_type):
		msg={
			"DBStat":"api_DBStat gets an error!",
			"rowsoper_total":"api_rowsoper_total gets an error!",
			"rowsoper_ps":"api rowsoper_ps gets an error!",
			"innobuffer_memallco":"api innobuffer_memallco gets an error!",
			"page":"api page gets an error!",
			"pool":"api pool gets an error!",
			"ps":"api qps gets an error!",
			"variablestatus_used":"api_variablestatus_used gets an error!",
			"variablestatus_ration":"api_variablestatus_ration gets an error!",
			"flow_control_paused":"api_flow_control_paused gets an error!",
			"slowest_node_param":"api_slowest_node_params gets an error!",
			"slowest_network_param":"api_slowest_network_param gets an error!"
		}
		return msg.get(api_type, "unknown api type!")
class TestDB(unittest.TestCase):

	container_ip=""
	global null
		
	def setUp(self):
		pass		
	def test_db_all_stat(self):		
		print "DBStat"        
		r = requests.get('http://%s:8888/db/all/stat'%self.container_ip)
		print r.text		
		code=eval(r.text)["meta"]["code"]
		self.assertEqual(200, code,get_err_msg("DBStat"))    
	def test_stat_rowsoper_total(self):
		print "rowsoper/total"        
		r = requests.get('http://%s:8888/db/all/stat/rowsoper/total'%self.container_ip)
		print r.text 
		code=eval(r.text)["meta"]["code"]
		self.assertEqual(200,code,get_err_msg("rowsoper_total"))				
	def test_stat_rowsoper_ps(self):		
		print "rowsoper/ps"        
		r = requests.get('http://%s:8888/db/all/stat/rowsoper/ps'%self.container_ip)
		print r.text
		code=eval(r.text)["meta"]["code"]
		self.assertEqual(200,code,get_err_msg("rowsoper_ps"))		
	def test_stat_innobuffer_memallco(self):		
		print "memallco"        
		r = requests.get('http://%s:8888/db/all/stat/innobuffer/memallco'%self.container_ip)
		print r.text
		code=eval(r.text)["meta"]["code"]
		self.assertEqual(200, code, get_err_msg("innobuffer_memallco"))		
	
	def test_stat_innobuffer_page(self):		
		print "page"        
		r = requests.get('http://%s:8888/db/all/stat/innobuffer/page'%self.container_ip)
		print r.text
		code=eval(r.text)["meta"]["code"]
		self.assertEqual(200,code,get_err_msg("page"))		

	
	def test_stat_innobuffer_pool(self):		
		print "pool"        
		r = requests.get('http://%s:8888/db/all/stat/innobuffer/pool'%self.container_ip)
		print r.text
		code=eval(r.text)["meta"]["code"]
		self.assertEqual(200,code,get_err_msg("pool"))

	
	def test_stat_variablestatus_ps(self):		
		print "QPS"        
		r = requests.get('http://%s:8888/db/all/stat/variablestatus/ps'%self.container_ip)
		print r.text
		code=eval(r.text)["meta"]["code"]
		self.assertEqual(200,code,get_err_msg("ps"))		

	
	def test_stat_variablestatus_used(self):		
		print "Thread_Cache_Used"        
		r = requests.get('http://%s:8888/db/all/stat/variablestatus/used'%self.container_ip)
		print r.text
		code=eval(r.text)["meta"]["code"]
		self.assertEqual(200,code,get_err_msg("variablestatus_used"))
	
	def test_stat_variablestatus_ration(self):	
		print "Ration"        
		r = requests.get('http://%s:8888/db/all/stat/variablestatus/ration'%self.container_ip)
		print r.text
		code=eval(r.text)["meta"]["code"]
		self.assertEqual(200,code,get_err_msg("variablestatus_ration"))		
		
	
	def test_stat_wsrepstatus_flow_control_paused(self):		
		print "flow_control_paused"        
		r = requests.get('http://%s:8888/db/all/stat/wsrepstatus/flow_control_paused'%self.container_ip)
		print r.text
		code=eval(r.text)["meta"]["code"]
		self.assertEqual(200,code,get_err_msg("flow_control_paused"))		
		
	def test_stat_wsrepstatus__slowest_node_param(self):		
		print "slowest_node_param" 
		r = requests.get('http://%s:8888/db/all/stat/wsrepstatus/slowest_node_param'%self.container_ip)
		print r.text
		code=eval(r.text)["meta"]["code"]
		self.assertEqual(200,code,get_err_msg("slowest_node_param"))
	
	def test_stat_wsrepstatus_slowest_network_param(self):		
		print "slowest_network_param"        
		r = requests.get('http://%s:8888/db/all/stat/wsrepstatus/slowest_network_param'%self.container_ip)
		print r.text
		code=eval(r.text)["meta"]["code"]
		self.assertEqual(200,code,get_err_msg("slowest_network_param"))	
	
	def tearDown(self):
		print "call this method is finish\n"
        
if __name__=="__main__":
	unittest.main()

import threading
import logging
import time

from common.zkOpers import ZkOpers
from common.utils.threading_exception_queue import Threading_Exception_Queue
from common.utils.mail import send_email
from common.configFileOpers import ConfigFileOpers
from common.invokeCommand import InvokeCommand
from tornado.options import options

class Abstract_Mysql_Service_Action_Thread(threading.Thread):
    
    threading_exception_queue = Threading_Exception_Queue()
    
    confOpers = ConfigFileOpers()
    
    invokeCommand = InvokeCommand()
    
    def __init__(self):
        threading.Thread.__init__(self)
        
    #duplicate Cluster_stop_action._check_stop_status
    def _check_stop_status(self, data_node_ip):
        zkOper = ZkOpers()
        while True:
            isLock = False
            try:
                isLock,lock = zkOper.lock_node_start_stop_action()
                break
            except:
                continue
            finally:
                if isLock:
                    zkOper.unLock_node_start_stop_action(lock)
                    zkOper.close()
                    
        stop_finished = False
        while not stop_finished:
            
            started_nodes = zkOper.retrieve_started_nodes()
            
            stop_finished = True
            for i in range(len(started_nodes)):
                started_node = started_nodes[i]
                if started_node == data_node_ip:
                    stop_finished = False
                    
            time.sleep(1)
            
        return stop_finished
    
    def _send_email(self, data_node_ip, text):
        try:
            # send email
            subject = "[%s] %s" % (data_node_ip, text)
            body = "[%s] %s" % (data_node_ip, text)
            
#            email_from = "%s <noreply@%s>" % (options.sitename, options.domain)
            if options.send_email_switch:
                send_email(options.admins, subject, body)
        except Exception,e:
            logging.error("send email process occurs error", e)   

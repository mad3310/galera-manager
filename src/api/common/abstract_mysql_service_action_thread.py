# -*- coding: utf-8 -*-

import threading
import logging

from common.utils.threading_exception_queue import Threading_Exception_Queue
from common.utils.mail import send_email
from common.configFileOpers import ConfigFileOpers
from common.invokeCommand import InvokeCommand
from tornado.options import options
from common.zkOpers import Watch_ZkOpers


class Abstract_Mysql_Service_Action_Thread(threading.Thread):

    threading_exception_queue = Threading_Exception_Queue()

    confOpers = ConfigFileOpers()

    invokeCommand = InvokeCommand()

    zkOper = None

    def __init__(self):
        threading.Thread.__init__(self)

    def retrieve_zkOper(self):
        if None == self.zkOper:
            self.zkOper = Watch_ZkOpers()

        return self.zkOper

    def _send_email(self, data_node_ip, text):
        try:
            # send email
            subject = "[%s] %s" % (data_node_ip, text)
            body = "[%s] %s" % (data_node_ip, text)

#            email_from = "%s <noreply@%s>" % (options.sitename, options.domain)
            if options.send_email_switch:
                send_email(options.admins, subject, body)
        except Exception, e:
            logging.error("send email process occurs error", e)

import sys
import threading
import logging
import traceback

from tornado.options import options
from common.utils.mail import send_email
from tornado.web import HTTPError
from common.utils.exceptions import HTTPAPIError
from common.utils.threading_exception_queue import Threading_Exception_Queue
from common.helper import get_localhost_ip
from common.invokeCommand import InvokeCommand


class Thread_Exception_Handler_Worker(threading.Thread):
    
    threading_exception_queue = Threading_Exception_Queue()
    
    def __init__(self):
        super(Thread_Exception_Handler_Worker,self).__init__()
    
    def run(self):
        exc_info = None
        try:
            while not self.threading_exception_queue.empty(): 
                exc_info = self.threading_exception_queue.get(block=False)
                
                if exc_info is None:
                    continue
                
                e = exc_info[1]
    
                if isinstance(e, HTTPAPIError):
                    pass
                elif isinstance(e, HTTPError):
                    e = HTTPAPIError(e.status_code)
                else:
                    e = HTTPAPIError(500)
    
                exception = "".join([ln for ln in traceback.format_exception(*exc_info)])
    
                logging.error(e)
                self._send_error_email(exception)
        except:
            exc_type, exc_obj, exc_trace = exc_info
            # deal with the exception
            logging.error(exc_type)
            logging.error(exc_obj)
            logging.error(exc_trace)
            
    def _send_error_email(self, exception):
        try:
            # send email
            subject = "[%s]Internal Server Error" % options.sitename
#            body = self.render_string("errors/500_email.html", exception=exception)
            host_ip = get_localhost_ip()
            invokeCommand = InvokeCommand()
            version_str = invokeCommand._runSysCmd("rpm -qa container-manager")
            exception += "\n" + version_str[0] + "\nhost ip :" + host_ip
#            email_from = "%s <noreply@%s>" % (options.sitename, options.domain)
            if options.send_email_switch:
                send_email(options.admins, subject, exception + '')
        except Exception:
            logging.error(traceback.format_exc())

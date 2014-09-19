import Queue
import threading

class Threading_Exception_Queue(object):
    
    bucket = Queue.Queue()
    
    write_lock = threading.Lock()
    
    read_lock = threading.Lock()
        
    @property
    def queue(self):
        return self.bucket
    
    @property
    def queueLock(self):
        return self.queueLock
    
    def empty(self):
        result = False
        
        self.read_lock.acquire()
        try:
            result = self.bucket.empty()
        except Queue.Empty:
            pass
        finally:
            self.read_lock.release()
            
        return result
    
    def get(self, block=False):
        self.read_lock.acquire()
        try:
            if not self.bucket.empty():
                data = self.bucket.get(block)
        except Queue.Empty:
            pass
        finally:
            self.read_lock.release()
            
        return data
        
    def put(self, exception):
        self.write_lock.acquire()
        try:
            data = self.bucket.put(exception)
        finally:
            self.write_lock.release()
            
        return data
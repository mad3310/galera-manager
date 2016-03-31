import time
from common.status_opers import Check_DB_WR_Available
from handlers.monitor import Node_Info_Async_Handler, DB_Info_Async_Handler
from common.status_opers import Check_DB_Anti_Item


class Monitor_handle_Asyc(object):
    
    '''estimate unlocking zookeeper need time(secord)
    '''
    time_constant = 1
    
    def __init__(self):
        pass
    
    def _action_monitor_async(self, lock_name, data_node_info_list, begin_time, timeout):
        method_name = "_"  + lock_name.split('/')[1]
        getattr(self, method_name)(data_node_info_list)
        
        end_time = time.time()
        monitor_exc_time = int(end_time - begin_time)
        
        '''leave timeout for sleep
        '''
        real_time_out = timeout - self.time_constant
        
        if monitor_exc_time < real_time_out:
            time.sleep(real_time_out - monitor_exc_time)
            
                
    def _async_monitor_handler(self, data_node_info_list):
        node_handler = Node_Info_Async_Handler()
        db_handler = DB_Info_Async_Handler()
        
        node_handler.retrieve_info(data_node_info_list)
        db_handler.retrieve_info(data_node_info_list)

    def _async_monitor_anti(self, data_node_info_list):
        check_db_anti_itmes = Check_DB_Anti_Item()
        
        check_db_anti_itmes.check(data_node_info_list)
        
    def _async_monitor_write_read(self, data_node_info_list):
        check_db_wr_available = Check_DB_WR_Available()
        
        check_db_wr_available.check(data_node_info_list)

    
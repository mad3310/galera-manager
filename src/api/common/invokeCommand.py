#!/usr/bin/env python
#-*- coding: utf-8 -*-

import subprocess
import logging
from tornado.options import options

class InvokeCommand():
    
    def _runSysCmd(self,cmdStr):
        if cmdStr == "":
            return ("",0)
        p = subprocess.Popen(cmdStr, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        ret_str = p.stdout.read()
        retval = p.wait()
        return (ret_str.strip(),retval)

    def _runSysCmdnoWait(self,cmdStr):
        if cmdStr == "":
            return False
        try:
            p = subprocess.Popen(cmdStr, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        except Exception, e:
            return False
        if p.poll():
            return False
        else:
            return p.pid

    def runBootstrapScript(self):
        stop_command_result = self._runSysCmd(options.mysql_stop_command)
        if(stop_command_result[1] == 0):
            print stop_command_result[0]
            boot_strap_result = self._runSysCmd(options.mysql_boot_strape_script)
            if(boot_strap_result[1] == 0):
                boot_strap_text = boot_strap_result[0]
                targetText = "The sstuser password is "
                password_pos = boot_strap_text.find(targetText)
                sst_user_password = boot_strap_text[password_pos+len(targetText):password_pos+len(targetText)+8]
                return sst_user_password
            
    def run_service_mysql_start_script(self):
        restart_command_result = self._runSysCmd(options.mysql_restart_command)
        if(restart_command_result[1] == 0):
            logging.info(restart_command_result[0])
            return True
            
        return False
    
    def mysql_service_start(self, isNewCluster='False'):
        restart_command_result = None
        
        if isNewCluster == 'True':
            restart_command_result = self._runSysCmd(options.mysql_start_new_cluster_command)
        else:
            restart_command_result = self._runSysCmd(options.mysql_start_command)
            
        if(restart_command_result[1] == 0):
            logging.info(restart_command_result[0])
            return True
            
        return False
    
    def mysql_service_stop(self):
        restart_command_result = self._runSysCmd(options.mysql_stop_command)
        if(restart_command_result[1] == 0):
            logging.info(restart_command_result[0])
            return True
            
        return False
    
    def remove_mysql_socket(self):
        restart_command_result = self._runSysCmd(options.remove_mysql_socket)
        if(restart_command_result[1] == 0):
            logging.info(restart_command_result[0])
            return True
            
        return False
    
#     def run_service_mysql_status_script(self):
#         i = 1
#         while i<=200:
#             status_command_result = self._runSysCmd(options.mysql_status_command)
#             if(status_command_result[1] == 0):
#                 status_command_text = status_command_result[0]
#                 logging.info(status_command_text)
#                 target_text = "mcluster-mysqld (pid"
#                 boolean_result = status_command_text.startswith(target_text)
#                 if boolean_result:
#                     break;
#                 
#             i+=1
#         
#         if i==200:    
#             return False
#         
#         return True
    
    def run_check_shell(self, check_shell_path_name):
        check_shell_result = self._runSysCmd(check_shell_path_name)
        result = check_shell_result[0]
        logging.info("check shell: " +  check_shell_path_name + " the result is: " + result)
        return result

    def run_service_shell(self, service_shell_path_name):
        result = str(self._runSysCmdnoWait(service_shell_path_name))
        logging.info("service shell: " + service_shell_path_name + " the result is: " + result)
        return result
            
if __name__ == "__main__":
    invokeCommand = InvokeCommand()
    sst_user_password = invokeCommand.runBootstrapScript()
    print sst_user_password

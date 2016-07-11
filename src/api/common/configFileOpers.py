#!/usr/bin/env python
#-*- coding: utf-8 -*-

import os, re
from common.utils.exceptions import CommonException

class ConfigFileOpers(object):
    
    def getValue(self,fileName,keyList=None):
        resultValue = {}
        f = file(fileName, 'r')
        
        totalDict = {}
        
        try:
            while True:
                line = f.readline()
                
                if not line:
                    break
                
                pos1 = line.find('=')
                key = line[:pos1]
                value = line[pos1+1:len(line)].strip('\n')
                totalDict.setdefault(key,value)
        finally:
            f.close()
       
        if keyList == None:
            resultValue = totalDict
        else:
            for key in keyList:
                value = totalDict.get(key)
                resultValue.setdefault(key,value)
            
        return resultValue
    
    def setValue(self, fileName, keyValueMap):
        inputstream = open(fileName)
        lines = inputstream.readlines()
        inputstream.close()
        
        outputstream = open(fileName, 'w')
        
        textContents = []
        for line in lines:
            pos1 = line.find('=')
            targetKey = line[:pos1]
            resultLine = line
            
            if keyValueMap.has_key(targetKey):
                value = keyValueMap[targetKey]
                resultLine = targetKey + '=' + value + '\n'
                    
            textContents.append(resultLine)
                    
        outputstream.writelines(textContents)
                    
        outputstream.close()
        st = os.stat(fileName)
        os.chmod(fileName, st.st_mode)
        
    def retrieveFullText(self, fileName):
        inputstream = open(fileName)
        
        try:
            lines = inputstream.readlines()
        finally:
            inputstream.close()
        
        resultValue = ''
        for line in lines:
            resultValue += line
            
        return resultValue
    
    
    def writeFullText(self, fileName,fullText):
        if not os.path.exists(fileName):
            raise CommonException("%s file not existed!" % (fileName))
        
        outputstream = open(fileName,'w')
        outputstream.write('')
        outputstream.write(fullText)
        outputstream.close()
                    
    def ipFormatChk(self, ip_str):
        pattern = r"^(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[0-9]{1,2})(\.(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[0-9]{1,2})){3}$"
        if not re.match(pattern, ip_str):
            return True
        return False
    

if __name__ == "__main__":
    s = ConfigFileOpers()
    resultValue = s.getValue('C:/Users/asus/Downloads/my.cnf', ['wsrep_cluster_address','wsrep_sst_auth'])
    print resultValue
    
    s.setValue('C:/Users/asus/Downloads/my.cnf', {'wsrep_sst_auth':'zbz:zbz'})
    resultValue = s.getValue('C:/Users/asus/Downloads/my.cnf', ['wsrep_cluster_address','wsrep_sst_auth'])
    print resultValue
    
    resultValue = s.retrieveFullText('C:/Users/asus/Downloads/my.cnf')
    print resultValue

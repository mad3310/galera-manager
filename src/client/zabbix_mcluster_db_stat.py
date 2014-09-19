#-*- coding: utf-8 -*-

'''
Created on 2013-7-21

@author: asus
'''

import urllib2
import logging
import json
import sys

MCLUSTER_VIP = 'localhost'
stat_base_url = "http://%s:8888" % (MCLUSTER_VIP)

stat_point_url_mapping = {}
stat_point_url_mapping.setdefault("db.innodb_buffer.memalloc", "%s/db/all/stat/innobuffer/memallco" % stat_base_url)
stat_point_url_mapping.setdefault("db.innodb_buffer.page", "%s/db/all/stat/innobuffer/page" % stat_base_url)
stat_point_url_mapping.setdefault("db.innodb_buffer.pool", "%s/db/all/stat/innobuffer/pool" % stat_base_url)
stat_point_url_mapping.setdefault("db.variable_status.ps", "%s/db/all/stat/variablestatus/ps" % stat_base_url)
stat_point_url_mapping.setdefault("db.variable_status.ration", "%s/db/all/stat/variablestatus/ration" % stat_base_url)
stat_point_url_mapping.setdefault("db.variable_status.used", "%s/db/all/stat/variablestatus/used" % stat_base_url)
stat_point_url_mapping.setdefault("db.row_opers.ps", "%s/db/all/stat/rowsoper/ps" % stat_base_url)
stat_point_url_mapping.setdefault("db.row_opers.total", "%s/db/all/stat/rowsoper/total" % stat_base_url)
stat_point_url_mapping.setdefault("db.wsrep_status.flow_control_paused", "%s/db/all/stat/wsrepstatus/flow_control_paused" % stat_base_url)
stat_point_url_mapping.setdefault("db.wsrep_status.slowest_node_param", "%s/db/all/stat/wsrepstatus/slowest_node_param" % stat_base_url)
stat_point_url_mapping.setdefault("db.wsrep_status.slowest_network_param", "%s/db/all/stat/wsrepstatus/slowest_network_param" % stat_base_url)

def stat(stat_point):
    url = stat_point_url_mapping.get(stat_point)
    f = urllib2.urlopen(url)
    encodedjson =  f.read()
    monitor_return_json_value = json.loads(encodedjson)
    
    reponse_code = monitor_return_json_value['meta']['code']
    if reponse_code != 200:
        serious_dict.setdefault(url, "due to response code error, please check email to find the reason!")
        return
    
    dict = {}
    for stat_point in monitor_return_json_value['response']:
        return_value = monitor_return_json_value['response'][stat_point]
        dict.setdefault(stat_point,return_value)
        
    return dict
    
            
def convert_dict_to_str(dict):
    return_result = ''
    
    for (key, value) in dict.iteritems():
        sub_item = "%s\t%s\n" % (key, value)
        return_result += sub_item
        
    return_result = return_result.rstrip('\n')
    return return_result
    
    
def main():
    stat_type = sys.argv[1]
    stat_first_level = sys.argv[2]
    stat_second_level = sys.argv[3]
    
    stat_point = "%s.%s.%s" % (stat_type, stat_first_level, stat_second_level)
    dict = stat(stat_point)
    return_result = convert_dict_to_str(dict)
    
    print return_result

if __name__ == "__main__":
    main()

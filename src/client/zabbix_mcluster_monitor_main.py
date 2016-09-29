#-*- coding: utf-8 -*-

'''
Created on 2013-7-21

@author: asus
'''

import urllib2
import json

MCLUSTER_VIP = 'localhost'


def check_monitor_main(serious_dict, general_dict, nothing_dict):
    url = "http://%s:8888/mcluster/monitor" % (MCLUSTER_VIP)
    f = urllib2.urlopen(url)
    encodedjson = f.read()
    monitor_return_json_value = json.loads(encodedjson)

    reponse_code = monitor_return_json_value['meta']['code']
    if reponse_code != 200:
        serious_dict.setdefault(
            url, "due to response code error, please check email to find the reason!")
        return

    for monitor_type in monitor_return_json_value['response']:
        for monitor_item in monitor_return_json_value['response'][monitor_type]:
            monitor_type_item = monitor_type + "." + monitor_item
            logic_return_code = monitor_return_json_value[
                'response'][monitor_type][monitor_item]['alarm']
            logic_return_message = monitor_return_json_value[
                'response'][monitor_type][monitor_item]['message']

            if logic_return_code == 'tel:sms:email':
                serious_dict.setdefault(
                    monitor_type_item, logic_return_message)
            elif logic_return_code == 'sms:email':
                general_dict.setdefault(
                    monitor_type_item, logic_return_message)
            else:
                nothing_dict.setdefault(
                    monitor_type_item, logic_return_message)


def main():
    serious_dict = {}

    general_dict = {}

    nothing_dict = {}

    check_monitor_main(serious_dict, general_dict, nothing_dict)

    print "main_serious:%s" % serious_dict
    print "main_general:%s" % general_dict
    print "main_nothing:%s" % nothing_dict

if __name__ == "__main__":
    main()

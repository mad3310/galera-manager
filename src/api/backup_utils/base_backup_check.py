import urllib2, urllib
import json

FULL_LOG_FILE_PATH = '/var/log/mcluster-manager/mcluster-backup/'
INCR_LOG_FILE_PATH = '/var/log/mcluster-manager/mcluster-backup/incr/'

def get_response_request(host, url_path, data):
    url = "http://%s:8888%s" % (host, url_path)
    data = urllib.urlencode(data)
    f = urllib2.urlopen(url, data)
    encodedjson =  f.read()
    return json.loads(encodedjson)

def get_local_backup_status(full_type, time_path):
    result = False

    if full_type == 'full':
        full_file_name = FULL_LOG_FILE_PATH + time_path + '_script.log'
        with open(full_file_name, 'r') as f:
            for line in f.readlines():
                if line.find('== the full backup is completed ==') != -1:
                    result = True
            return result

    else:
        incr_file_name = INCR_LOG_FILE_PATH + time_path + '_incr_script.log'
        with open(incr_file_name, 'r') as f:
            for line in f.readlines():
                if line.find('== the incr backup is completed ==') != -1:
                    result = True
            return result


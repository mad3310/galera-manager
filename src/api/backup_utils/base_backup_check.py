import urllib2, urllib
import json
from common.utils.exceptions import HTTPAPIErrorException

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
        file_name = FULL_LOG_FILE_PATH + time_path + '_script.log'
    else:
        file_name = INCR_LOG_FILE_PATH + time_path + '_incr_script.log'
    
    try:  
        with open(file_name, 'r') as f:
            for line in f.readlines():
                if line.find('backup is completed ==') != -1:
                    result = True
            return result
    
    except Exception,e:
        raise HTTPAPIErrorException("no such a backup file", status_code=500)
    
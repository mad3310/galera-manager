 
import sys, time, json, traceback, commands, logging 
#import containerManager, global_varies, mailutil
import re
#conf_ini = global_varies.conf_ini
import unicodedata

logging.basicConfig(filename= 'test_case.log', level =logging.INFO)
def get_case(file_path):
    f = open(file_path)
    content = f.read()
    cases = json.loads(content)
    return cases

def _get_case_result(curl):
    curl = ' %s 2>/dev/null' % curl
    ret = commands.getoutput(curl)
    print '_get_case_result result:',str(ret)
    return json.loads(ret)

def compare_result(result, expect_result):
    if result == True:
        return True
    rst = True
    if len(result) != len(expect_result):
        rst = False
    for key, value in result.items():
        if key not in expect_result:
            rst = False
            break
        if value != expect_result.get(key):
            rst = False
            break
    return rst

def run_one_case(case):
    error_detail = ''
    if not isinstance(case, dict):
        error_detail = 'get a wrong case, please check!'
        return error_detail
    try: 
        curl = case.get('curl')
        expect_result = case.get('expect_result')
        print expect_result, type(expect_result), 22222
        expect_result = eval(expect_result)
        #init = case.get('init')
        logging.info('test: %s' % str(curl))
        print 'test: %s' % str(curl)
        ret = _get_case_result(curl)
        logging.info('test result : %s' % str(ret) )
        logging.info('expect result: %s' % str(expect_result))
        #com_ret = compare_result(ret, expect_result)
        #logging.info('compare reslut: %s' % str(com_ret))
        #print 'compare reslut: %s' % str(com_ret)
  #      if not com_ret:
  #          error_detail = r'interface: %s: \n test result: %s, expect result: %s' % (curl, str(ret),  str(expect_result) )
  #          logging.error(error_detail)
  #          return error_detail
    except:
        logging.error(str(traceback.format_exc()))
        return str(traceback.format_exc())

def get_key_via_value(pattern_str, expect_result_dict):
    response_dict = expect_result_dict.get("response")
    target_key = None
    reg_expression = None
    for key , value in response_dict.items():
        if pattern_str in value:
            target_key = key
            reg_expression = value.replace(pattern_str, "")
            break
    if target_key == None:
        return None
    tem_list = []
    tem_list.append(target_key)
    tem_list.append(reg_expression)
    return tem_list 

def m_run_one_case(case):
    error_detail = ''
    pattern_str = "regular_expression:" 
    if not isinstance(case, dict):
        error_detail = 'get a wrong case, please check!'
        return error_detail
    try: 
        curl = case.get('curl')
        expect_result = case.get('expect_result')
        print expect_result, type(expect_result), 22222
        #expect_result = eval(expect_result)
        ret_list = [] 
        ret_list = get_key_via_value(pattern_str, expect_result)
        #init = case.get('init')
        logging.info('test: %s' % str(curl))
        print 'test: %s' % str(curl)
        ret = _get_case_result(curl)
        logging.info(str(ret))
       #  reg_pattern = expect_result.get("response").get("reg_exp_pattern") 
        if  ret_list != None:
            reg_pattern = ret_list[1]
            reg_instance = str(ret.get("response").get(ret_list[0]))
            logging.info("reg_instance : %s", reg_instance)
            reg_pattern_ascii = reg_pattern.encode("ascii", "ignore")
            ret = (None != re.match(reg_pattern_ascii, reg_instance))
            logging.info(str(ret_list))
            logging.info((reg_pattern))
            logging.info(str(ret_list[0]))
       
        logging.info('test result : %s' % str(ret) )
        logging.info('expect result: %s' % str(expect_result))
        com_ret = compare_result(ret, expect_result)
        logging.info('compare reslut: %s' % str(com_ret))
        #print 'compare reslut: %s' % str(com_ret)
  #      if not com_ret:
  #          error_detail = r'interface: %s: \n test result: %s, expect result: %s' % (curl, str(ret),  str(expect_result) )
  #          logging.error(error_detail)
  #          return error_detail
    except:
        logging.error(str(traceback.format_exc()))
        return str(traceback.format_exc())


def run_cases(file_path, end_step = -1):
    try:
        case_index_list = []
        cases = get_case(file_path)
        for case_index in cases:
            case_index_list.append(case_index)
        case_index_list.sort()
        count = 0
        if end_step == -1: 
            end_step = len(case_index_list)
        for case_index in case_index_list:
            case = cases.get(case_index)
            error_detail = m_run_one_case(case)
            count = count + 1
            if count == end_step:
                break
      #      if error_detail:
      #          mailutil.sendMail('auto interface notice', error_detail)
      #          return False
      #  mailutil.sendMail('auto interface notice', 'pass')
        return True
    except:
        logging.error( str(traceback.format_exc()) )
        print str(traceback.format_exc()), 11111
        return False

#def check_vm_stat():
#    node_ips = []
#    node_ips = conf_ini.get('Vagrantfile').get('node_ips')
#    
#    while True:
#        stat = True
#        succ = []
#        for index, host_ip in enumerate(node_ips):
#            print 'begin host_ip:%s' % host_ip  
#            cm = containerManager.ContainerManager(host_ip)
#            ret = cm.container_manager_status()
#            logging.info('host_ip:%s, result: %s' % (host_ip, str(ret)) )
#            print 'host_ip:%s, result: %s' % (host_ip, str(ret))
#            if ret:
#                succ.append(host_ip)
#            else:
#                stat = False
#        logging.info('start result: %s' % stat)
#        print 'start result: %s' % stat
#        if stat:
#            logging.info('successfult, please test your server interface!')
#            print 'successful, please test your server interface!'
#            return True
#        
#        for host_ip in succ:
#            node_ips.remove(host_ip)
#        time.sleep(3)
#
#def handleTimeout(func, timeout, *params, **paramMap):
#    interval = 0.6
#    if type(timeout) == tuple:
#        timeout, interval = timeout
#    rst = None
#    while timeout > 0:
#        t = time.time()
#        rst = func(*params, **paramMap)
#        if rst and not _isExcept(rst):
#            break
#        time.sleep(interval)
#        timeout -= time.time() - t
#    return rst
#
##def _isExcept(e, eType = Exception):
#    return isinstance(e, eType)

if __name__ == '__main__':
    print str(sys.argv)
    if len(sys.argv) == 1:
        print get_case('test_a.json')
        run_cases('test_a.json')
    elif len(sys.argv) == 2:
        end_step = int(sys.argv[1])
        run_cases('test_a.json', end_step)
    else:
        print "Arguments Error"
	#print get_case('test.json') 

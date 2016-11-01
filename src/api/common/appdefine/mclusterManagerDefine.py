# -*- coding: utf-8 -*-
import os

from tornado.options import define
from .consts import ADMIN_EMALS

join = os.path.join
dirname = os.path.dirname

base_dir = os.path.abspath(dirname(dirname(dirname(__file__))))


define('port', default=8888, type=int, help='app listen port')
define('debug', default=False, type=bool, help='is debuging?')
define('sitename', default="mcluster manager", help='site name')
define('domain', default="letv.com", help='domain name')

define('send_email_switch', default=True, type=bool, help='the flag of if send error email')
define('admins', default=ADMIN_EMALS, help='admin email address')
define('smtp_host', default="10.205.91.22", help='smtp host')
define('smtp_port', default=587, help='smtp port')
define('smtp_user', default="mcluster", help='smtp user')
define('smtp_password', default="Mcl_20140903!", help='smtp password')
define('smtp_from_address', default='mcluster@letv.com', help='smtp from address')
define('smtp_duration', default=10000, type=int, help='smtp duration')
define('smtp_tls', default=False, type=bool, help='smtp tls')


define("base_dir", default=base_dir, help="project base dir")
define("mysql_cnf_file_name", default="/opt/letv/mcluster/root/etc/my.cnf", help="mysql cnf file name")
define("mcluster_manager_cnf", default=join(base_dir, "config", "mclusterManager.cnf"), help="mcluster manager config file")
define("data_node_property", default=join(base_dir, "config", "dataNode.property"), help="data node config file")
define('zk_address', default=join(base_dir, "config", "mclusterManager.cnf"), help="zookeeper address config file")
define("cluster_property", default=join(base_dir, "config", "cluster.property"), help="cluster config file")


define("start_gbalancer", default="nohup gbalancer --config=%s %s >/dev/null 2>&1 &", help="start gbalancer")
define("glb_json_file_name", default="/etc/gbalancer/%sconfiguration.json", help="gbalancer config file")

define("check_datanode_health", default=join(base_dir, "shell", "check_datanode_health.sh"), help="check datanode health")
define("check_datanode_error", default=join(base_dir, "shell", "check_datanode_error.sh"), help="check datanode error")
define("check_datanode_warning", default=join(base_dir, "shell", "check_datanode_warning.sh"), help="check datanode warning")
define("kill_innotop", default=join(base_dir, "shell", "kill_innotop.sh"), help="kill innotop")


define("mysql_host", default="127.0.0.1", help="mcluster database host")
define("mysql_port", default=3306, type=int, help="mcluster database port")
define("mysql_start_command", default="service mcluster-mysqld start", help="mysql start command")
define("remove_mysql_socket", default="rm /var/lib/mysql/mysql.sock", help="remove mysql socket")
define("mysql_start_new_cluster_command", default="service mcluster-mysqld start --wsrep-cluster-address='gcomm://'", help="mysql start new cluster command")
define("mysql_restart_command", default="service mcluster-mysqld restart", help="mysql restart command")
define("mysql_stop_command", default="service mcluster-mysqld stop", help="mysql stop command")
define("mysql_status_command", default="service mcluster-mysqld status", help="mysql status command")
define("mysql_boot_strape_script", default="/usr/share/mysql/mcluster-bootstrap", help="mysql boot strape script position")
define("retrieve_node_uuid_seqno_script", default=join(base_dir, "shell", "retrieve_node_uuid_seqno.sh"), help="retrieve node uuid and seqno")


define("alarm_serious", default="tel:sms:email", help="alarm level is serious")
define("alarm_general", default="sms:email", help="alarm level is general")
define("alarm_nothing", default="nothing", help="no alarm")

define("stat_rows_oper", default="/usr/local/bin/innotop -h127.0.0.1 -uroot -pMcluster --nonint --count 1 --mode R", help="stat_rows_oper")
define("stat_innodb_buffer", default="/usr/local/bin/innotop -h127.0.0.1 -uroot -pMcluster --nonint --count 1 --mode B", help="stat_innodb_buffer")
define("stat_variable_status", default="/usr/local/bin/innotop -h127.0.0.1 -uroot -pMcluster --nonint --count 1 --mode S", help="stat_variable_status")
define("stat_top_command", default='top -u mysql -bn 1', help='stat_top_command')
define("stat_mem_command", default='free -g', help='stat_mem_command')

define("sleep_time", default=10, type=int, help="sleep time in _check_start_status")
define("delta_time", default=8, type=int, help="delta time between read and write")

define("new_count_times", default=60, type=int, help="if it is new cluster, we use this by default")
define("old_count_times", default=300, type=int, help="Others we use this value.")
define("full_back_sh", default=join(base_dir, "shell", "mcluster_backup_full.sh"), help="full backup")
define("on_check_storedprocedure", default=False, type=bool, help='the flag of checking stored procedure')
define("es_hosts", default='10.140.67.117:9200,10.140.67.85:9200,10.140.67.86:9200', help='elasticsearch hosts')
define("es_version", default=2, help='elasticsearch hosts')

define("S3_ACCESS_KEY", default='EH18VA68TUPMOF4L5MK3')
define("S3_ACCESS_SECRET", default='Y3KW8LAyVcTNS1cAnPEv847lUmtFXILVg+8gXaIo')
define("S3_BUCKET_NAME", default='martrix')
define("S3_PREFIX_URL", default='http://s3.lecloud.com/')

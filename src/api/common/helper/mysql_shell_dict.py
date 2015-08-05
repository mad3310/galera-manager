MYSQL_SHELL_DICT = {
                    "stat_version_command" : 'mysql -uroot -pMcluster -e "select version()"|grep -v "-"| grep -v "Var"|grep -v \'version\' | awk \'{print "version\t"$1 }\'',
                    "stat_wsrep_status_command" : 'mysql -uroot -pMcluster -e "show status like \'wsrep_cluster_status\'"|grep -v "-"| grep -v "Var"',
                    
                    "stat_running_day_command" : 'mysql -uroot -pMcluster -e "show global status like \'uptime\'"|grep -v "-"| grep -v "Var"',
                    "stat_connection_count_command" : 'mysql -uroot -pMcluster -e "show status like \'Threads_connected\'"|grep -v "-"| grep -v "Var"',
                    
                    "stat_active_count_command" : 'mysql -uroot -pMcluster -e "show status like \'Threads_running\'"|grep -v "-"| grep -v "Var"',
                    "stat_wating_count_command" : 'mysql -uroot -pMcluster -e "show processlist"|grep -v \'Rows_examined\' | grep -v \'Sleep\' |awk \'$6 > 2\'|wc -l| awk \'{print "wait_num\t"$1 }\'',
                    "stat_net_send_command" : 'mysql -uroot -pMcluster -e "show global status like \'Bytes_sent\'"|grep -v "-"| grep -v "Var"',
                    "stat_net_rev_command" : 'mysql -uroot -pMcluster -e "show global status like \'Bytes_received\'"|grep -v "-"| grep -v "Var"',
                    "stat_QPS_command" : 'mysql -uroot -pMcluster -e "show global status where variable_name in(\'com_select\')"|grep -v "-"| grep -v "Var"',
                    "stat_Com_commit_command" : 'mysql -uroot -pMcluster -e "show global status like \'Com_commit\'"|grep -v "-"| grep -v "Var"',
                    
                    "stat_Com_rollback" : 'mysql -uroot -pMcluster -e "show global status like \'Com_rollback\'"|grep -v "-"| grep -v "Var"',
                    "stat_slow_query_command" : 'mysql -uroot -pMcluster -e "show global status like \'Slow_queries\'"|grep -v "-"| grep -v "Var"',
                    "stat_key_blocks_used_command" : 'mysql -uroot -pMcluster -e "show status like \'key_blocks_used\'"',

                    "stat_max_conn_command" : 'mysql -uroot -pMcluster -e "show variables like \'max_connections\'"|grep -v "-"| grep -v "Var"',
                    "stat_max_err_conn_command" : 'mysql -uroot -pMcluster -e "show variables like \'max_connect_errors\'"|grep -v "-"| grep -v "Var"',
                    "stat_max_open_file_command" : 'mysql -uroot -pMcluster -e "show variables like \'open_files_limit\'"|grep -v "-"| grep -v "Var"',
                    
                    "stat_opened_file_command" : 'mysql -uroot -pMcluster -e "show status like \'Open_files\'"|grep -v "-"| grep -v "Var"',
                    "stat_table_cach_command" : 'mysql -uroot -pMcluster -e "show variables like \'table_open_cache\'"|grep -v "-"| grep -v "Var"',
                    "stat_opened_table_cach_command" : 'mysql -uroot -pMcluster -e "show status like \'Open_tables\'"|grep -v "-"| grep -v "Var"',
                    "stat_table_cach_noha_command" : 'mysql -uroot -pMcluster -e "show status like \'Opened_tables\'"|grep -v "-"| grep -v "Var"',
                    
                    "stat_key_buffer_size_command" : 'mysql -uroot -pMcluster -e "show variables like \'key_buffer_size\'"|grep -v "-"| grep -v "Var"',
                    "stat_sort_buffer_size_command" : 'mysql -uroot -pMcluster -e "show variables like \'sort_buffer_size\'"|grep -v "-"| grep -v "Var"',
                    "stat_join_buffer_size_command" : 'mysql -uroot -pMcluster -e "show variables like \'join_buffer_size\'"|grep -v "-"| grep -v "Var"',
                   
                    "stat_key_blocks_unused_command" : 'mysql -uroot -pMcluster -e "show status like \'Key_blocks_unused\'"|grep -v "-"| grep -v "Var"',
                    "stat_key_blocks_used_command" : 'mysql -uroot -pMcluster -e "show status like \'Key_blocks_used\'"|grep -v "-"| grep -v "Var"',
                    "stat_key_blocks_not_flushed_command" : 'mysql -uroot -pMcluster -e "show status like \'Key_blocks_not_flushed\'"|grep -v "-"| grep -v "Var"',

                    "stat_key_buffer_reads_command" : 'mysql -uroot -pMcluster -e "show status like \'key_reads\'"|grep -v "-"| grep -v "Var"',
                    "stat_key_buffer_reads_request_command" : 'mysql -uroot -pMcluster -e "show status like \'key_read_requests\'"|grep -v "-"| grep -v "Var"',
                    "stat_key_buffer_writes_command" : 'mysql -uroot -pMcluster -e "show status like \'key_writes\'"|grep -v "-"| grep -v "Var"',
                    "stat_key_buffer_writes_request_command" : 'mysql -uroot -pMcluster -e "show status like \'key_write_requests\'"|grep -v "-"| grep -v "Var"',

                    "stat_innodb_bufferpool_size_command" : 'mysql -uroot -pMcluster -e "show variables like \'innodb_buffer_pool_size\'"|grep -v "-"| grep -v "Var"',
                    "stat_innodb_bufferpool_reads_command" : 'mysql -uroot -pMcluster -e "show status like \'innodb_buffer_pool_reads\'"|grep -v "-"| grep -v "Var"',
                    "stat_innodb_bufferpool_read_request_command" : 'mysql -uroot -pMcluster -e "show status like \'innodb_buffer_pool_read_requests\'"|grep -v "-"| grep -v "Var"',
                    
                    "stat_database_size_command" : 'du -kP /srv/mcluster/mysql/{0}',

                    "stat_innodb_dml_command" : 'mysql -uroot -pMcluster -e "show engine innodb status\G;"|grep "inserts/s"',
                    "stat_table_space_analyze_command" : 'mysql -uroot -pMcluster -Be "select TABLE_SCHEMA, table_name, table_comment, (data_length+index_length)/1024 as total_kb from information_schema.tables where table_schema=\'{0}\'"',
                    
                    "stat_wsrep_local_fail_command" : 'mysql -uroot -pMcluster -Bse "show status like \'wsrep_local_cert_failures\'"',
                    "stat_wsrep_local_bf_aborts_command" : 'mysql -uroot -pMcluster -Bse "show status like \'wsrep_local_bf_aborts\'"',
                    "stat_wsrep_local_replays_command" : 'mysql -uroot -pMcluster -Bse "show status like \'wsrep_local_replays\'"',
                    
                    "stat_wsrep_replicated_command" : 'mysql -uroot -pMcluster -Bse "show status like \'wsrep_replicated\'"',
                    "stat_wsrep_replicated_bytes_command" : 'mysql -uroot -pMcluster -Bse "show status like \'wsrep_replicated_bytes\'"',
                    "stat_wsrep_received_command" : 'mysql -uroot -pMcluster -Bse "show status like \'wsrep_received\'"',
                    "stat_wsrep_received_bytes_command" : 'mysql -uroot -pMcluster -Bse "show status like \'wsrep_received_bytes\'"',
                    
                    "stat_wsrep_flow_control_paused_command" : 'mysql -uroot -pMcluster -Bse "show status like \'wsrep_flow_control_paused\'"',
                    "stat_wsrep_flow_control_sent_command" : 'mysql -uroot -pMcluster -Bse "show status like \'wsrep_flow_control_sent\'"',
                    "stat_wsrep_flow_control_recv_command" : 'mysql -uroot -pMcluster -Bse "show status like \'wsrep_flow_control_recv\'"',
                    
                    }

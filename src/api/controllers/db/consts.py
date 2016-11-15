# -*- coding: utf-8 -*-

# PT-OSC工具测试命令
PT_TEST_COMMAND = ("pt-online-schema-change -uroot -pMcluster -h127.0.0.1 "
                   "--alter={sqls} --dry-run D={db_name},t={tb_name}")
# PT-OSC工具命令
PT_COMMAND = ("pt-online-schema-change -uroot -pMcluster -h127.0.0.1 --no-drop-old-table "
              "--alter={sqls} --execute D={db_name},t={tb_name}")

# -*- coding: utf-8 -*-


class BackupStatus():
    _status = None

    def __init__(self):
        _status = ['process_start',
                   'backup_start',
                   'backup_finish',
                   'backup_fail',
                   'trans_start',
                   'trans_finish',
                   'trans_fail',
                   'process_fail',
                   'process_end']

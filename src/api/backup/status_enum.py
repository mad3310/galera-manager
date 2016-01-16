#!/usr/bin/env python
#-*- coding: utf-8 -*-

class Enum(set):
    def __getattr__(self, name):
        if name in self:
            return name
        raise AttributeError

_status = ['backup_starting', 'backup_failed', 'backup_succecced', 'backup_transmit_succeed', 'backup_transmit_faild']

Status = Enum(_status)

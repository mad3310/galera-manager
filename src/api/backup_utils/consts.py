# -*- coding: utf-8 -*-

from utils.storify import storify

BACKUP_TYPE = storify(dict(
    FULL='full',
    INCR='incr',
))

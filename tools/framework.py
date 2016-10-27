# -*- coding: utf-8 -*-

import os
import sys


def init():
    PATH = os.path.dirname(os.path.realpath(__file__))
    PATH = os.path.dirname(PATH)

    sys.path.append(PATH)

    reload(sys)
    sys.setdefaultencoding('utf8')

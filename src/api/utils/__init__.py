# -*- coding: utf-8 -*-

import re


def check_ip(self, ip):
    regx = r"^(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[0-9]{1,2}) \
            (\.(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[0-9]{1,2})){3}$"
    return True if not re.match(regx, ip) else False

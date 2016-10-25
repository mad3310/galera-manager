# -*- coding: utf-8 -*-

from src.api.utils import check_ip

from .framework import BaseTestCase

VALID_IP = '255.155.255.111'
INVALID_IP = '192.168..11.1s'


class UtilsTest(BaseTestCase):
    def test_ip_check(self):
        r = check_ip(VALID_IP)
        assert r
        r = check_ip(INVALID_IP)
        assert not r

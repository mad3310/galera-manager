# -*- coding: utf-8 -*-

import os
import binascii
from base64 import b64encode


def randbytes(bytes):
    """Return bits of random data as a hex string."""
    return binascii.hexlify(os.urandom(bytes))


def randbytes2(bytes):
    return b64encode(randbytes(bytes)).rstrip('=')

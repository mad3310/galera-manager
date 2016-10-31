# -*- coding: utf-8 -*-

import os
import binascii
from base64 import b64encode


if hasattr(os, 'urandom'):
    def randbytes(bytes):
        """Return bits of random data as a hex string."""
        return binascii.hexlify(os.urandom(bytes))
elif os.path.exists('/dev/urandom'):
    def randbytes(bytes):
        """Return bits of random data as a hex string."""
        return binascii.hexlify(open("/dev/urandom").read(bytes))


def randbytes2(bytes):
    return b64encode(randbytes(bytes)).rstrip('=')

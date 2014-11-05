#-*- coding: utf-8 -*-

import random
import string 


def get_random_password():
    a = list(string.letters+string.digits)
    random.shuffle(a)
    random.shuffle(a)
    random.shuffle(a)
    return "".join(a[:8])
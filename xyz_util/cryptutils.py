# -*- coding:utf-8 -*- 
# author = 'denishuang'
from __future__ import unicode_literals
from django.conf import settings
from Crypto.Cipher import DES
from base64 import b64decode, b64encode

if hasattr(settings, "DES_ECB_KEY"):
    DES_ECB_KEY = settings.DES_ECB_KEY
else:
    DES_ECB_KEY = "abcdefgh"

BS = DES.block_size
pad = lambda s: s + (BS - len(s) % BS) * chr(BS - len(s) % BS)
unpad = lambda s: s[0:-ord(s[-1])]


def des_encrypt(value, key=DES_ECB_KEY):
    if value is None:
        return value
    des = DES.new(key, DES.MODE_ECB)
    return b64encode(des.encrypt(pad(value.encode())))


def des_decrypt(value, key=DES_ECB_KEY):
    if value is None:
        return value
    s = b64decode(value)
    des = DES.new(key, DES.MODE_ECB)
    return unpad(des.decrypt(s))


def get_mac_address():
    import uuid
    node = uuid.getnode()
    mac = uuid.UUID(int=node).hex[-12:]
    return mac

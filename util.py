import hashlib
import time
import re
from struct import *
import random
from datetime import datetime
import sqlite3

def gen_id():
    h = hashlib.new('ripemd160')
    h.update(str(time.time()))
    return h.digest()

def hexstr(data):
    import binascii
    return binascii.b2a_hex(data)

def binstr(s):
    import binascii
    return binascii.a2b_hex(s)

def bencode(obj):
    otype = type(obj)
    if otype == str:
        return "%d:%s" % (len(obj), obj)
    elif otype == int:
        return "i%se" % (str(obj))
    elif otype == list:
        ret = "l"
        for i in obj:
            ret += bencode(i)
        ret += "e"
        return ret
    elif otype == dict:
        ret = "d"
        keys = obj.keys()
        keys.sort()
        for key in keys:
            ret += bencode(key)
            ret += bencode(obj[key])
        ret += "e"
        return ret
    else:
        return None

def bdecode(s):
    try:
        if s[0] == 'i':
            e = s.index('e')
            return int(s[1:e]), s[e+1:]
        elif s[0] >= '0' and s[0] <= '9':
            m = re.search(r'\d+', s)
            slen = int(m.group(0))
            idx = s.index(':')+1
            return s[idx:idx+slen], s[idx+slen:]
        elif s[0] == 'l':
            ret = []
            s = s[1:]
            while s[0] and s[0] != 'e':
                d, s = bdecode(s)
                ret.append(d)
            return ret, s[1:]
        elif s[0] == 'd':
            ret = {}
            s = s[1:]
            while s[0] and s[0] != 'e':
                k, s = bdecode(s)
                v, s = bdecode(s)
                ret[k] = v
            return ret, s[1:]
        else:
            return None
    except Exception, e:
        # raise e
        return None

def gentid():
    return pack('>I', int(random.random()*(2**32)))

def decompact(info):
    if len(info) != 6:
        return None, None
    ip, port = unpack('>IH', info)
    ip = '.'.join(map(str, [ip>>24, (ip>>16)&0xff, (ip>>8)&0xff, ip&0xff]))
    return ip, port

def _test():
    print hexstr(gen_id())
    print repr(binstr('abcd'))
    print hexstr(gentid())
    print decompact('\x7f\x00\x00\x01\x12\x34')
    print bencode({'a':[1,2,'0']})
    print bdecode(bencode({'a':[1,2,'0']}))

if __name__ == '__main__':
    _test()
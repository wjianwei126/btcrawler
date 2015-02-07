#!/usr/env python

from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor
from twisted.internet.task import LoopingCall

import hashlib
import time
import re
from struct import *
import random
from datetime import datetime
from util import *
from utp import *
import sys

class Task:

    piece_size = 16384

    def __init__(self, proto, info_hash, (host, port)):
        self.id = proto.id
        self.info_hash = info_hash
        self.host = host
        self.port = port
        self.buf = ''
        self.handshake = False
        self.extension = False
        self.prepared = False
        self.conn = uTPConnection(proto, self.utp_receive, (host, port));
        self.conn.connect(self.conn.send, self.handshake_package())
        self.metadata = 2
        self.metasize = 0
        self.result = []
        self.start_time = time.time()

    def receive(self, data):
        # print "receive: %d" % (len(data))
        self.conn.receive(data)

    def utp_receive(self, data):
        self.buf += data

        if self.handshake == False and len(self.buf) > 68:
            if self.buf[0] != '\x13' or self.buf[1:20].lower() != 'bittorrent protocol':
                # print 'protocol not support'
                # print repr(self.buf[0:20])
                self.conn.reset()
                self.buf = ''
                return
            # print "protocol: %s" % (self.buf[1:20])
            self.buf = self.buf[20:]
            # print "reserved: %s" % (hexstr(self.buf[0:8]))
            self.buf = self.buf[8:]
            # print "infohash: %s" % (hexstr(self.buf[0:20]))
            self.buf = self.buf[20:]
            # print "peerid: %s" % (hexstr(self.buf[0:20]))
            self.buf = self.buf[20:]
            self.handshake = True
        if self.handshake and self.extension == False:
            self.conn.send(self.extension_package())
            self.extension = True
        if self.extension and self.prepared == False:
            while len(self.buf) > 4:
                dlen = unpack('>I', self.buf[0:4])[0]
                if len(self.buf) < (dlen+4):
                    break
                msg = self.buf[4:dlen+4]
                self.buf = self.buf[dlen+4:]
                if msg[0] == '\x05':
                    self.conn.send(self.bitfield_package(len(msg)))
                if msg[0] == '\x14':
                    result = bdecode(msg[2:])
                    if result == None:
                        continue
                    bmsg, rm = result
                    if 'metadata_size' in bmsg:
                        self.metasize = bmsg['metadata_size']
                    if 'm' in bmsg and 'ut_metadata' in bmsg['m']:
                        self.metadata = bmsg['m']['ut_metadata']
                        self.conn.send(self.request(0))
                    if 'msg_type' in bmsg and bmsg['msg_type'] == 1:
                        if 'total_size' in bmsg:
                            self.metasize = bmsg['total_size']
                        if not 'piece' in bmsg:
                            continue
                        pcnt = int(self.metasize / Task.piece_size)
                        if self.metasize % Task.piece_size > 0:
                            pcnt += 1
                        while len(self.result) < pcnt:
                            self.result.append(None)
                        self.result[bmsg['piece']] = rm
                        self.prepared = True
                        for i in xrange(1, pcnt):
                            self.conn.send(self.request(i))
        if self.prepared:
            while len(self.buf) > 4:
                dlen = unpack('>I', self.buf[0:4])[0]
                if len(self.buf) < (dlen+4):
                    break
                msg = self.buf[4:dlen+4]
                self.buf = self.buf[dlen+4:]
                if msg[0] != '\x14':
                    continue
                result = bdecode(msg[2:])
                if result == None:
                    continue
                bmsg, rm = result
                if not 'piece' in bmsg:
                    continue
                self.result[bmsg['piece']] = rm

    def handshake_package(self):
        reserved = '\x00\x00\x00\x00\x00\x10\x00\x05'
        h = pack('>B19s8s20s20s', 19, 'BitTorrent protocol', reserved, self.info_hash, self.id)
        return h

    def bitfield_package(self, blen):
        bf = '\x05' + '\x00' * (blen-1)
        return pack('>I%ss'%(blen), blen, bf)

    def extension_package(self):
        msg = {
            'm': {'ut_metadata': 2},
        }
        bmsg = bencode(msg)
        mlen = len(bmsg) + 2
        ret = pack('>IBB', mlen, 20, 0) + bmsg
        return ret

    def request(self, piece):
        msg = {
            'msg_type': 0,
            'piece': piece
        }
        bhs = bencode(msg)
        mlen = len(bhs)+2
        ret = pack('>IBB', mlen, 20, 2) + bhs
        return ret

    def update(self):
        self.conn.update()

    def check_result(self):
        if len(self.result) == 0:
            return False
        for piece in self.result:
            if piece == None:
                return False
        info = ''.join(self.result)
        sha1 = hashlib.sha1()
        sha1.update(info)
        # print info
        return sha1.digest() == self.info_hash

    def status(self):
        if self.conn.state == 'close' or time.time() - self.start_time > 60:
            return 'fail'
        if self.check_result():
            return 'success'
        return 'working'

    def get_result(self):
        if self.check_result():
            return ''.join(self.result)
        else:
            return None

class GetinfoProtocol(DatagramProtocol):

    default_timeout = 300

    def __init__(self, info_hash, bootnodes=()):
        self.id = gen_id()
        self.info_hash = info_hash
        self.sessions = {}
        self.nodes = {}
        self.unvisitednodes = []
        self.bootnodes = bootnodes
        self.tasks = {}
        self.start_time = time.time()
        self.result = None

    def startProtocol(self):
        for host, port in self.bootnodes:
            self.find_node(self.id, (host, port))
        self.lc = LoopingCall(self.loop)
        self.lc.start(1)

    def stopProtocol(self):
        self.lc.stop()

    def write(self, ip, port, data):
        self.transport.write(data, (ip, port))

    def datagramReceived(self, data, (host, port)):
        data = bytes(data)
        bd = bdecode(data)
        if bd == None or data[0] != 'd':
            if (host, port) in self.tasks:
                self.tasks[(host, port)].receive(data)
            return
        rmsg, rm = bd
        if not 't' in rmsg:
            return
        tid = rmsg['t']
        if rmsg['y'] == 'r':
            if (tid in self.sessions) == False:
                return
            mtype = self.sessions[tid]
            del self.sessions[tid]
            if mtype == 'ping':
                self.nodes[rmsg['r']['id']] = (host, port)
            elif mtype == 'find_node':
                self.handle_rfindnode(rmsg)
            elif mtype == 'get_peers':
                self.handle_rgetpeers(rmsg)
                pass
            elif mtype == 'announce_peer':
                pass
        elif rmsg['y'] == 'q':
            if rmsg['q'] == 'ping':
                self.nodes[rmsg['a']['id']] = (host, port)
                self.rping(tid, (host, port))
            elif rmsg['q'] == 'find_node':
                self.rfind_node(tid, rmsg['a']['target'], (host, port))
            elif rmsg['q'] == 'get_peers':
                self.rget_peers(tid, rmsg['a']['info_hash'], (host, port))
            elif rmsg['q'] == 'announce_peer':
                self.rannounce_peer(tid, (host, port))

    def find_node(self, target, (host, port)):
        tid = gentid()
        self.sessions[tid] = 'find_node'
        msg = {
            "t": tid,
            "y": "q",
            "q": "find_node",
            "a": {
                "id": self.id,
                "target": target,
            }
        }
        bmsg = bencode(msg)
        reactor.resolve(host).addCallback(self.write, port, bmsg)

    def rfind_node(self, tid, target, (host, port)):
        nodes = ''
        k = 8
        for i in self.nodes:
            if k == 0:
                break
            k -= 1
            h, p = self.nodes[i]
            nodes += i
            bytes = map(int, h.split('.'))
            for b in bytes:
                nodes += pack('B', b)
            nodes += pack('>H', p)
        msg = {
            "t": tid,
            "y": "r",
            "r": {
                "id": self.id,
                "nodes": nodes,
            }
        }
        bmsg = bencode(msg)
        reactor.resolve(host).addCallback(self.write, port, bmsg)

    def ping(self, (host, port)):
        tid = gentid()
        self.sessions[tid] = 'ping'
        msg = {
            "t": tid,
            "y": "q",
            "q": "ping",
            "a": {
                "id": self.id
            }
        }
        bmsg = bencode(msg)
        reactor.resolve(host).addCallback(self.write, port, bmsg)

    def rping(self, tid, (host, port)):
        msg = {
            "t": tid,
            "y": "r",
            "r": {
                "id": self.id
            }
        }
        bmsg = bencode(msg)
        reactor.resolve(host).addCallback(self.write, port, bmsg)

    def get_peers(self, info_hash, (host, port)):
        tid = gentid()
        self.sessions[tid] = 'get_peers'
        msg = {
            "t": tid,
            "y": "q",
            "q": "get_peers",
            "a": {
                "id": self.id,
                "info_hash": info_hash
            }
        }
        bmsg = bencode(msg)
        reactor.resolve(host).addCallback(self.write, port, bmsg) 

    def rget_peers(self, tid, info_hash, (host, port)):
        nodes = ''
        k = 8
        for i in self.nodes:
            if k == 0:
                break
            k -= 1
            h, p = self.nodes[i]
            nodes += i
            bytes = map(int, h.split('.'))
            for b in bytes:
                nodes += pack('B', b)
            nodes += pack('>H', p)
        msg = {
            "t": tid,
            "y": "r",
            "r": {
                "id": self.id,
                "token": gen_id(),
                "nodes": nodes,
            }
        }
        bmsg = bencode(msg)
        reactor.resolve(host).addCallback(self.write, port, bmsg)

    def announce_peer(self, info_hash, (host, port)):
        # TODO
        pass

    def rannounce_peer(self, tid, (host, port)):
        msg = {
            "t": tid,
            "y": "r",
            "r": {
                "id": self.id
            }
        }
        bmsg = bencode(msg)
        reactor.resolve(host).addCallback(self.write, port, bmsg)

    def handle_rfindnode(self, rmsg):
        if ('nodes' in rmsg['r']) == False:
            return
        nodes = rmsg['r']['nodes']
        
        for i in xrange(0, len(nodes), 26):
            info = nodes[i:i+26]
            nid, compact = unpack('>20s6s', info)
            ip, port = decompact(compact)
            if (nid in self.nodes) == False:
                # self.find_node(gen_id(), (ip, port))
                # reactor.callLater(random.random()*60, self.find_node, gen_id(), (ip, port))
                self.unvisitednodes.append((ip, port))
            self.nodes[nid] = (ip, port)

    def handle_rgetpeers(self, rmsg):
        if not 'r' in rmsg:
            return
        if 'nodes' in rmsg['r']:
            for i in xrange(0, len(rmsg['r']['nodes']), 26):
                nid, compact = unpack('>20s6s', rmsg['r']['nodes'][i:i+26])
                ip, port = decompact(compact)
                self.unvisitednodes.append((ip, port))
        if 'values' in rmsg['r']:
            for peer in rmsg['r']['values']:
                host, port = decompact(peer)
                if host == None or port == None:
                    continue
                if not (host, port) in self.tasks:
                    # print "new peer: %s:%d" % (host, port)
                    self.tasks[(host, port)] = Task(self, self.info_hash, (host, port))

    def timeout(self):
        return time.time()-self.start_time>GetinfoProtocol.default_timeout

    def finish(self):
        return self.result != None

    def get_result(self):
        return self.result

    def loop(self):
        t = 4
        while t > 0:
            t -= 1
            if len(self.unvisitednodes) == 0:
                break
            host, port = self.unvisitednodes.pop()
            # self.find_node(gen_id(), (host, port))
            self.get_peers(self.info_hash, (host, port))
        for task in self.tasks.values():
            task.update()
            if task.status() == 'fail':
                del self.tasks[(task.host, task.port)]
            elif task.status() == 'success':
                self.result = task.get_result()
                self.tasks = {}

def test():
    boots = (('router.bittorrent.com', 6881),)
    p = GetinfoProtocol(binstr('83ec090801a3aec2486df67cc9f529231d8fdafb'), boots)
    port = reactor.listenUDP(6881, p)
    reactor.run()

if __name__ == '__main__':
    test()
    pass
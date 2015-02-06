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
from hashdb import *

class BittorrentProtocol(DatagramProtocol):

    def __init__(self, bootnodes=()):
        self.id = gen_id()
        self.sessions = {}
        self.nodes = {}
        self.unvisitednodes = []
        for host, port in bootnodes:
            self.unvisitednodes.append((host, port))
        self.hashdb = HashDB()
        self.sshash = {}
        self.hashnodes = []

    def startProtocol(self):
        # for host, port in self.bootnodes:
            # self.find_node(self.id, (host, port))
        pass

    def stopProtocol(self):
        self.hashdb.release()

    def write(self, ip, port, data):
        self.transport.write(data, (ip, port))

    def datagramReceived(self, data, (host, port)):
        data = bytes(data)
        bd = bdecode(data)
        if bd == None:
            # self.taskmgr.receive(data, (host, port))
            return
        rmsg, rm = bd
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
                # reactor.callLater(int(random.random()*10), self.handle_rfindnode, rmsg)
            elif mtype == 'get_peers':
                # if tid in self.sshash:
                #     self.handle_rgetpeers(self.sshash[tid], rmsg)
                #     del self.sshash[tid]
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
                h = hexstr(rmsg['a']['info_hash'])
                self.hashdb.insert_hash(h)
                # if self.hashdb.exist(h) == False:
                #     self.unknownhashes.append(rmsg['a']['info_hash'], host, port)
            elif rmsg['q'] == 'announce_peer':
                self.rannounce_peer(tid, (host, port))
                h = hexstr(rmsg['a']['info_hash'])
                self.hashdb.insert_hash(h)
                # if self.hashdb.exist(h) == False:
                #     self.unknownhashes.append(rmsg['a']['info_hash'], host, port)

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
        self.sshash[tid] = info_hash
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

    def handle_rgetpeers(self, info_hash, rmsg):
        if 'nodes' in rmsg['r']:
            for i in xrange(0, len(rmsg['r']['nodes']), 26):
                nid, compact = unpack('>20s6s', rmsg['r']['nodes'][i:i+26])
                ip, port = decompact(compact)
                self.hashnodes.append((info_hash, ip, port))
        if 'values' in rmsg['r']:
            for compact in rmsg['r']['values']:
                if len(compact) != 6:
                    continue
                ip, port = decompact(compact)
                # self.taskmgr.new_task(info_hash, ip, port)
                print hexstr(info_hash), ip, port

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

    def loop(self):
        t = 16
        while t > 0:
            t -= 1
            if len(self.unvisitednodes) == 0:
                break
            host, port = self.unvisitednodes.pop()
            self.find_node(gen_id(), (host, port))

def monitor(p):
    print "[%s] got %d nodes" % (datetime.now(), len(p.nodes))

def main():
    boots = (('router.bittorrent.com', 6881),)
    p = BittorrentProtocol(boots)
    lc = LoopingCall(monitor, p)
    lc.start(5)
    lf = LoopingCall(p.loop)
    lf.start(5)
    reactor.listenUDP(6881, p)
    reactor.run()

if __name__ == '__main__':
    main()

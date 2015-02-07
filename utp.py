
import random
import time
from struct import *

class uTPConnection:

	state = ('close', 'connected', 'fin')
	mtype = ('DATA', 'FIN', 'STATE', 'RST', 'SYN')
	default_version = 1
	default_wnd_size = 0xf000
	max_buffer_size = 0x100000

	class Buffer:
		def __init__(self):
			self.buf = {}
			self.min_seq = None
			self.max_seq = None
			self.ack = None

		def put(self, seq, data, ack=False):
			if self.min_seq == None and self.max_seq == None:
				self.min_seq = seq
				self.max_seq = seq
				if ack:
					self.ack = seq
				self.buf[seq] = (data, ack)
			elif seq > self.max_seq:
				for i in xrange(self.max_seq+1, seq):
					self.buf[i] = ('', False)
				self.buf[seq] = (data, ack)
				self.max_seq = seq
				while (self.min_seq in self.buf) == False:
					self.min_seq += 1
			elif seq <= self.max_seq and (seq in self.buf):
				del self.buf[seq]
				self.buf[seq] = (data, ack)
			if ack:
				while self.ack < self.max_seq and self.buf[self.ack+1][1]:
					self.ack += 1
			if self.max_seq and self.max_seq-self.min_seq >= uTPConnection.max_buffer_size:
				self.resize()

		def get(self, key):
			if key in self.buf:
				return self.buf[key]
			else:
				return '', False

		def state(self, seq):
			if seq >= self.min_seq and seq <= self.max_seq and seq in self.buf:
				data, ack = self.buf[seq]
				self.buf[seq] = (data, True)
				if self.ack == None and seq != self.min_seq:
					return
				if self.ack == None:
					self.ack = seq
				else:
					if seq > self.ack:
						while self.ack < self.max_seq and self.buf[self.ack+1][1]:
							self.ack += 1

		def resize(self):
			if self.ack > self.min_seq:
				for i in xrange(self.min_seq, self.ack):
					del self.buf[i]
					self.min_seq += 1

		def size(self):
			if self.min_seq != None:
				return (self.max_seq - self.min_seq) + 1
			return 0

		def pop(self):
			if self.min_seq != None and self.min_seq in self.buf and self.buf[self.min_seq][1]:
				data = self.buf[self.min_seq][0]
				del self.buf[self.min_seq]
				if self.min_seq < self.max_seq:
					self.min_seq += 1
				if len(data) > 0:
					return data
			return None

	def __init__(self, proto, onreceive, (host, port)):
		self.proto = proto
		self.on_receive = onreceive
		self.host = host
		self.port = port
		self.version = uTPConnection.default_version
		self.state = 'close'
		self.send_buf = uTPConnection.Buffer()
		self.recv_buf = uTPConnection.Buffer()
		self.wnd_size = uTPConnection.default_wnd_size
		self.seq = random.getrandbits(16)
		self.ack = 0
		self.start_time = self.__timestamp_now()
		self.time_diff = 0
		self.recv_id = 0
		self.send_id = 0
		self.on_connect = None
		self.last_recv = 0

	def __timestamp_now(self):
		return int(time.time()*1000000)

	def __seq(self, data):
		return unpack('>H', data[16:18])[0]

	def __ack(self, data):
		return unpack('>H', data[18:20])[0]

	def __ext(self, data):
		return ord(data[1])

	def __timestamp(self, data):
		return unpack('>I', data[4:8])[0]

	def __send(self, data):
		self.seq += 1
		self.send_buf.put(self.__seq(data), data, False)

	def __make_header(self, stype, ext=None):
		mtype = uTPConnection.mtype.index(stype)
		tv = (mtype<<4)|(self.version&0x0f)
		ext = 0
		cid = self.send_id
		if stype == 'SYN':
			cid = self.recv_id
		tm = self.__timestamp_now()-self.start_time
		td = self.time_diff
		wnd_size = self.wnd_size
		seq = self.seq
		ack = self.ack
		hdr = pack('>BBHIIIHH', tv, ext, cid, tm, td, wnd_size, seq, ack)
		return hdr

	def __parse_header(self, data):
		tv = ord(data[0])
		mtype = (tv >> 4)
		version = (tv & 0x0f)
		result = [mtype, version]
		result.extend(unpack('>BHIIIHH', data[1:20]))
		return tuple(result)

	def __accept(self, data):
		mtype, ver, ext, cid, tm, td, wnd_size, seq, ack = self.__parse_header(data)
		if uTPConnection.mtype[mtype] != 'SYN' or ver != self.version:
			return
		self.recv_id = cid + 1
		self.send_id = cid
		self.ack = seq
		self.state = 'connected'
		self.__send_state(ack)

	def __send_state(self, ack):
		self.recv_buf.state(ack)
		hdr = self.__make_header('STATE')
		hdr = hdr[0:18] + pack('>H', ack)
		self.__send_now(hdr)

	def __recv_state(self, data):
		self.send_buf.state(self.__ack(data))

	def __recv_data(self, data):
		self.__send_state(self.__seq(data))
		ext = self.__ext(data)
		seq = self.__seq(data)
		data = data[20:]
		if ext != 0:
			while len(data) > 1:
				etype = ord(data[0])
				elen = ord(data[1])
				data = data[elen+2:]
				if etype == 0:
					break
		self.recv_buf.put(seq, data, True)
		msg = self.readmsg()
		if msg:
			self.on_receive(msg)

	def __establish(self, data):
		self.state = 'connected'
		if self.on_connect:
			callback, args = self.on_connect
			callback(*args)

	def __send_now(self, data):
		# print 'send', repr(data)
		self.proto.transport.write(data, (self.host, self.port))

	def connect(self, callback, *args):
		if self.state != 'close':
			raise 'connection is in use'
		self.recv_id = random.getrandbits(16)
		self.send_id = self.recv_id + 1
		pkt = self.__make_header('SYN')
		self.__send(pkt)
		self.state = 'syn_sent'
		self.on_connect = (callback, args)

	def close(self):
		self.state = 'fin'
		self.__send_now(self.__make_header('FIN'))

	def reset(self):
		self.state = 'close'
		self.send_buf.buf.clear()
		self.recv_buf.buf.clear()
		self.__send_now(self.__make_header('RST'))

	def receive(self, data):
		if len(data) < 20:
			raise 'incomplete package'
		r = True
		self.time_diff = abs(self.__timestamp_now() - self.start_time - self.__timestamp(data))
		tv = ord(data[0])
		mtype = (tv >> 4)
		if uTPConnection.mtype[mtype] == 'RST':
			self.reset()
		elif self.state == 'close' and uTPConnection.mtype[mtype] == 'SYN':
			self.__accept(data)
		elif self.state == 'syn_sent' and uTPConnection.mtype[mtype] == 'STATE':
			if self.__ack(data) == self.seq-1:
				self.__recv_state(data)
				self.__establish(data)
		elif self.state == 'fin' and uTPConnection.mtype[mtype] == 'DATA':
			self.__recv_data(data)
		elif self.state == 'connected':
			if uTPConnection.mtype[mtype] == 'DATA':
				self.__recv_data(data)
			elif uTPConnection.mtype[mtype] == 'RST':
				self.reset()
			elif uTPConnection.mtype[mtype] == 'FIN':
				self.close()
			elif uTPConnection.mtype[mtype] == 'STATE':
				self.__recv_state(data)
		else:
			r = False
		if r:
			self.last_recv = self.__timestamp_now()

	def send(self, data):
		hdr = self.__make_header('DATA')
		self.__send(hdr+data)

	def readmsg(self):
		return self.recv_buf.pop()

	def update(self):
		# print 'state', self.state
		if self.state == 'close':
			return
		if (self.state == 'connected' or self.state == 'fin') and self.__timestamp_now() - self.last_recv > 5000000:
			self.reset()
			return
		if self.state == 'syn_sent' or self.state == 'connected':
			# print self.send_buf.buf
			low = 0
			if self.send_buf.ack == None:
				low = self.send_buf.min_seq
			else:
				low = self.ack + 1
			# print low, self.send_buf.min_seq, self.send_buf.max_seq, self.send_buf.ack
			for i in xrange(low, self.send_buf.max_seq+1):
				data, ack = self.send_buf.get(i)
				if ack == False and len(data) >= 20:
					self.__send_now(data)

if __name__ == '__main__':
	pass

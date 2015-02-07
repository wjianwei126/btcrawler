
import sys
from util import *

def usage():
	print 'usage: python extract.py <filename>'
	print

def main(filename):
	f = open(filename, 'rb')
	data = f.read()
	f.close()
	info, rm = bdecode(data)
	if 'name' in info:
		print info['name']
	if 'files' in info:
		for f in info['files']:
			if 'path' in f:
				print f['path']

if __name__ == '__main__':
	if len(sys.argv) < 2:
		usage()
	else:
		main(sys.argv[1])
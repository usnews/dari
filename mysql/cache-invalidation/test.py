import sys
import binascii
import zmq
import struct

# Socket to talk to server
context = zmq.Context()
socket = context.socket(zmq.SUB)

print "Collecting updates from cache server"
socket.connect ("tcp://localhost:5556")

socket.setsockopt(zmq.SUBSCRIBE, "")

while 1:
    string = socket.recv()

    if string[0] == 'C':
        timestamp = struct.unpack_from("<d", string, 17)[0]
        
        print string[0], binascii.hexlify(string[1:17]), timestamp
        print("%.3f" % timestamp)

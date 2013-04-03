import sys
import binascii
import zmq

# Socket to talk to server
context = zmq.Context()
socket = context.socket(zmq.SUB)

print "Collecting updates from cache server"
socket.connect ("tcp://localhost:5556")

socket.setsockopt(zmq.SUBSCRIBE, "")

while 1:
    string = socket.recv()
    print binascii.hexlify(string)

#!/home/akayunov/wc/vienv/bin/python3
import sys
sys.path = ['/home/akayunov/wc/wc/lib'] + sys.path

import socket
from pika import frame
import amqp_spec
import amqp_types
import wc_interface

HOST = 'localhost'
PORT = '5672'
channel_number = amqp_types.ShortUint(1)

res = socket.getaddrinfo(HOST, PORT, socket.AF_INET, socket.SOCK_STREAM)
af, socktype, proto, canonname, sa = res[0]
c_socket = socket.socket(af, socktype, proto)
c_socket.connect(sa)

c_socket.send(amqp_spec.ProtocolHeader().encoded)
data = c_socket.recv(4096)
print(frame.decode_frame(data))


start_ok = wc_interface.start_ok('localhost', 'PLAIN', 'root', 'privetserver', 'en_US')
c_socket.send(start_ok.encoded)
data2 = c_socket.recv(4096)
print(frame.decode_frame(data2))


tune_ok = wc_interface.tune_ok(0, 131072, 5)
c_socket.send(tune_ok.encoded)
#data3 = c_socket.recv(4096)
#print(frame.decode_frame(data3))


open = wc_interface.conection_open('/')
c_socket.send(open.encoded)
data4 = c_socket.recv(4096)
print(frame.decode_frame(data4))

hearbeat_frame = wc_interface.hearbeat()
c_socket.send(hearbeat_frame.encoded)
data5 = c_socket.recv(4096)
print(frame.decode_frame(data5))


open = wc_interface.channel_open(channel_number)
c_socket.send(open.encoded)
data5 = c_socket.recv(4096)
print(frame.decode_frame(data5))


flow = wc_interface.flow(channel_number, 1)
c_socket.send(flow.encoded)
data6 = c_socket.recv(4096)
print(frame.decode_frame(data6))


declare = wc_interface.exchange_declare(channel_number, 'message', 'topic', 1, 1, 0, 0, 0, {})
c_socket.send(declare.encoded)
data7 = c_socket.recv(4096)
print(frame.decode_frame(data7))


declare_q = wc_interface.queue_declare(channel_number, 'text', 0, 1, 0, 0, 0, {})
c_socket.send(declare_q.encoded)
data8 = c_socket.recv(4096)
print(frame.decode_frame(data8))



declare_q = wc_interface.queue_bind(channel_number, 'text', 'message', 'text.#', 1, {})
c_socket.send(declare_q.encoded)
data8 = c_socket.recv(4096)
print(frame.decode_frame(data8))


for i in wc_interface.publish(channel_number, 'message', 'text.tratata', 0, 0, 'qrqwrq', {'content-type': 'application/json'}):
    c_socket.send(i.encoded)
    data8 = c_socket.recv(4096)
    print(frame.decode_frame(data8))

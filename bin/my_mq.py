#!/home/akayunov/wc/vienv/bin/python3
import sys
sys.path = ['/home/akayunov/wc/wc/lib'] + sys.path

import time
import socket
from pika import frame
import myspec

HOST = 'localhost'
PORT = '5672'

res = socket.getaddrinfo(HOST, PORT, socket.AF_INET, socket.SOCK_STREAM)
af, socktype, proto, canonname, sa = res[0]
c_socket = socket.socket(af, socktype, proto)
c_socket.connect(sa)
c_socket.send(myspec.ProtocolHeader().encoded)
data = c_socket.recv(4096)
print(frame.decode_frame(data))


client_properties = myspec.FieldTable({'host': myspec.LongString('localhost')})
mechanizm = myspec.ShortString('PLAIN')
response = myspec.LongString((myspec.Octet(0) + b'root' + myspec.Octet(0) + b'privetserver'))
locale = myspec.ShortString('en_US')
start_ok = myspec.Connection.StartOk([client_properties, mechanizm, response, locale])
print(start_ok)
c_socket.send(start_ok.encoded)
data2 = c_socket.recv(4096)
print(frame.decode_frame(data2))


channel_max = myspec.ShortUint(0)
frame_max = myspec.LongUint(131072)
heartbeat = myspec.ShortUint(5)
tune_ok = myspec.Connection.TuneOk([channel_max, frame_max, heartbeat])
print(tune_ok)
c_socket.send(tune_ok.encoded)
#data3 = c_socket.recv(4096)
#print(frame.decode_frame(data3))


virtual_host = myspec.ShortString('/')
reserved_str = myspec.ShortString('')
reserved_bit = myspec.Octet(0)
open = myspec.Connection.Open([virtual_host, reserved_str, reserved_bit])
print(open)
c_socket.send(open.encoded)
data4 = c_socket.recv(4096)
print(frame.decode_frame(data4))

hearbeat_frame = myspec.Heartbeat()
print(hearbeat_frame.encoded)
c_socket.send(hearbeat_frame.encoded)
data5 = c_socket.recv(4096)
print(frame.decode_frame(data5))
time.sleep(1)


reserved_str = myspec.ShortString('')
open = myspec.Channel.Open([reserved_str])
print(open)
c_socket.send(open.encoded)
data5 = c_socket.recv(4096)
print(frame.decode_frame(data5))


flow_start = myspec.Octet(1)
flow = myspec.Channel.Flow([flow_start])
print(flow)
c_socket.send(flow.encoded)
data6 = c_socket.recv(4096)
print(frame.decode_frame(data6))





# declare existing
reserved_short = myspec.ShortUint(1)
exchange_name = myspec.ShortString('message')
exchange_type = myspec.ShortString('topic')
# >> if bit go together when pack it in one octet
# 11000000 -> reverse order -> 00000011 -> 3
# exchange_passive,exchange_durable,exchange_auto_deleted,exchange_internal,exchange_no_wait
bits = myspec.Octet(3)
# <<< if bit go together when pack it in one octet
exchange_table = myspec.FieldTable({})
declare = myspec.Exchange.Declare([reserved_short, exchange_name, exchange_type, bits, exchange_table])
print(declare)
c_socket.send(declare.encoded)
data7 = c_socket.recv(4096)
print(frame.decode_frame(data7))




# queue declare
reserved_short = myspec.ShortUint(1)
queue_name = myspec.ShortString('text')
# >> if bit go together when pack it in one octet
# 11000000 -> reverse order -> 00000011 -> 3
# queue_passive,queue_durable,queue_exclusive,queue_auto_deleted,queue_no_wait
bits = myspec.Octet(2)
# <<< if bit go together when pack it in one octet
queue_table = myspec.FieldTable({})
declare_q = myspec.Queue.Declare([reserved_short, queue_name, bits, queue_table])
print(declare_q)
c_socket.send(declare_q.encoded)
data8 = c_socket.recv(4096)
print(frame.decode_frame(data8))



# queue bind
    # work too long
    # reserved_short = myspec.ShortUint(1)
    # queue_name = myspec.ShortString('text')
    # exchange_name = myspec.ShortString('message')
    # routing_key = myspec.ShortString('text.#')
    # # >> if bit go together when pack it in one octet
    # # 11000000 -> reverse order -> 00000011 -> 3
    # # no_wait
    # bits = myspec.Octet(1)
    # # <<< if bit go together when pack it in one octet
    # b_table = myspec.FieldTable({})
    # declare_q = myspec.Queue.Bind([reserved_short, queue_name, exchange_name, routing_key, bits, b_table])
    # print(declare_q)
    # c_socket.send(declare_q.encoded)
    # data8 = c_socket.recv(4096)
    # print(frame.decode_frame(data8))


# basic publish
reserved_short = myspec.ShortUint(1)
exchange_name = myspec.ShortString('message')
routing_key = myspec.ShortString('text.tratata')
# >> if bit go together when pack it in one octet
# 11000000 -> reverse order -> 00000011 -> 3
# no_wait
bits = myspec.Octet(0)
# <<< if bit go together when pack it in one octet
declare_q = myspec.Basic.Publish([reserved_short, exchange_name, routing_key, bits])
print(declare_q)
c_socket.send(declare_q.encoded)
#data8 = c_socket.recv(4096)
#print(frame.decode_frame(data8))

# content header
content_string = 'qrqwrq'
channel_number = myspec.ShortUint(1)
class_id = myspec.Basic.Publish.class_id
weight = myspec.ShortUint(0)
#import pdb;pdb.set_trace()
body_size = myspec.LongLongUint(len(content_string))
# property by order first property - highest bit 1000000000000000 - only first property
property_flag = myspec.ShortUint(32768)
property_values = myspec.ShortString('application/json')
header_publish = myspec.Header(channel_number, [class_id, weight, body_size, property_flag, property_values])
time.sleep(1)
print(header_publish)
c_socket.send(header_publish.encoded)
#data8 = c_socket.recv(4096)
#print(frame.decode_frame(data8))


content = myspec.Content(channel_number, content_string)
print(content)
c_socket.send(content.encoded)
data8 = c_socket.recv(4096)
print(frame.decode_frame(data8))
time.sleep(1)
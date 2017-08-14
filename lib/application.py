import socket
import amqp_spec
import time

from collections import deque
from ioloop import IOLoop
from exceptions import SfwException


class Application:

    def __init__(self):
        self.output_buffer_frames = deque()
        self.output_buffer = None
        self.buffer_in = b''

    def parse_buffer(self):
        frame, buffer_in = amqp_spec.decode_frame(self.buffer_in)
        print('IN: ' + str(int(time.time())) + ' ' + str(frame))
        self.buffer_in = buffer_in
        return frame

    def write(self, value):
        print('OUT:' + str(int(time.time())) + ' ' + str(value))
        self.output_buffer_frames.append(value)
        IOLoop.current().modify_to_write()

    # TODO we need to devide diferent channales for diferent coroutines
    def handle_read(self, by_timeout=False):
        if by_timeout:
            self.processor.send(None)
            return
        else:
            self.buffer_in += self.socket.recv(4096)
            frame = self.parse_buffer()
            if frame:
                if type(frame) is amqp_spec.Heartbeat:
                    self.write(amqp_spec.Heartbeat())
                else:
                    self.processor.send(frame)
                    return
            else:
                raise SfwException('Internal', 'Uknown handle read')

    def handle_write(self):
        # TODO use more optimize structure for slice to avoid copping
        if not self.output_buffer:
            frame = self.output_buffer_frames.popleft()
            self.output_buffer = [type(frame), frame.encoded]
        writed_bytes = self.socket.send(self.output_buffer[1])
        self.output_buffer[1] = self.output_buffer[1][writed_bytes:]
        if not self.output_buffer[1]:
            # if there is another frame in byffer it mean caller don't wait responce and do some self.write(frame) without waiting response
            try:
                frame = self.output_buffer_frames.popleft()
            except IndexError:
                frame = None
            if frame:
                self.output_buffer = [type(frame), frame.encoded]
            else:
                IOLoop.current().modify_to_read()
                if self.output_buffer[0].dont_wait_response:
                    self.processor.send(None)
                self.output_buffer = None

    def sleep(self, n):
        # TODO sleep it handle_write with timeout in deed!!
        writed_bytes = self.socket.send(self.output_buffer[1])
        self.output_buffer[1] = self.output_buffer[1][writed_bytes:]
        IOLoop.current().modify_to_read(timeout_in_seconds=n)

    def start(self):
        HOST = 'localhost'
        PORT = '5672'
        res = socket.getaddrinfo(HOST, PORT, socket.AF_INET, socket.SOCK_STREAM)
        af, socktype, proto, canonname, sa = res[0]
        self.socket = socket.socket(af, socktype, proto)
        self.socket.connect(sa)

        return self.socket

    def processor(self):
        protocol_header = amqp_spec.ProtocolHeader('A', 'M', 'Q', 'P', 0, 0, 9, 1)
        data = yield self.write(protocol_header)


        channel_number = 1
        start_ok = amqp_spec.Connection.StartOk({'host': ['S', 'localhost']}, 'PLAIN', credential=['root', 'privetserver'])
        tune = yield self.write(start_ok)


        tune_ok = amqp_spec.Connection.TuneOk(heartbeat_interval=100)
        # yield self.write(tune_ok)  # it works too!!!!
        self.write(tune_ok)  # it works too!!!!

        open = amqp_spec.Connection.Open(virtual_host='/')
        openok = yield self.write(open)


        open = amqp_spec.Channel.Open(channel_number=channel_number)
        openok = yield self.write(open)


        flow = amqp_spec.Channel.Flow(channel_number=channel_number)
        flowok = yield self.write(flow)


        declare = amqp_spec.Exchange.Declare('message', channel_number=channel_number)
        declareok = yield self.write(declare)

        declare_q = amqp_spec.Queue.Declare(queue_name='text', channel_number=channel_number)
        declareok = yield self.write(declare_q)

        declare_q = amqp_spec.Queue.Bind(queue_name='text', exchange_name='message', routing_key='text.#', channel_number=channel_number)
        bindok = yield self.write(declare_q)

        content = 'qrqwrq'
        r = [
            amqp_spec.Basic.Publish(exchange_name='message', routing_key='text.tratata', channel_number=channel_number),
            amqp_spec.Header(class_id=amqp_spec.Basic.Publish.class_id, body_size=len(content), properties={'content-type': 'application/json'}, channel_number=channel_number),
            amqp_spec.Content(content=content.encode('utf8'), channel_number=channel_number)
        ]
        publish_methods = r
        for i in publish_methods:
            yield self.write(i)
            print(1111111)
            yield self.sleep(3)
        # while 1:
        #     data9 = yield
        #     if type(data9) == amqp_spec.Heartbeat:
        #         yield self.write(amqp_spec.Heartbeat())

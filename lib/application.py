import socket
import amqp_spec
from ioloop import IOLoop


class Application:

    def __init__(self):
        self.output_buffer = b''
        self.buffer_in = b''

    def parse_buffer(self):
        frame, buffer_in = amqp_spec.decode_frame(self.buffer_in)
        print('IN: ' + str(frame))
        self.buffer_in = buffer_in
        return frame

    def write(self, value):
        print('OUT:' + str(value))
        self.output_buffer += value.encoded
        IOLoop.current().modify_to_write()

    # TODO we need to devide diferent channales for diferent coroutines
    def handle_read(self):
        self.buffer_in += self.socket.recv(4096)
        frame = self.parse_buffer()
        if frame:
            self.processor.send(frame)

    def handle_write(self):
        # TODO use more optimize structure for slice to avoid copping
        writed_bytes = self.socket.send(self.output_buffer)
        self.output_buffer = self.output_buffer[writed_bytes:]
        if not self.output_buffer:
            IOLoop.current().modify_to_read()

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
        self.write(start_ok)
        tune = yield

        tune_ok = amqp_spec.Connection.TuneOk(heartbeat_interval=4)
        self.write(tune_ok)
        open = amqp_spec.Connection.Open(virtual_host='/')
        self.write(open)
        openok = yield

        open = amqp_spec.Channel.Open(channel_number=channel_number)
        self.write(open)
        openok = yield

        flow = amqp_spec.Channel.Flow(channel_number=channel_number)
        self.write(flow)
        flowok = yield

        declare = amqp_spec.Exchange.Declare('message', channel_number=channel_number)
        self.write(declare)
        declareok = yield

        declare_q = amqp_spec.Queue.Declare(queue_name='text', channel_number=channel_number)
        self.write(declare_q)
        declareok = yield

        declare_q = amqp_spec.Queue.Bind(queue_name='text', exchange_name='message', routing_key='text.#', channel_number=channel_number)
        self.write(declare_q)
        bindok = yield
        content = 'qrqwrq'
        r = [
            amqp_spec.Basic.Publish(exchange_name='message', routing_key='text.tratata', channel_number=channel_number),
            amqp_spec.Header(class_id=amqp_spec.Basic.Publish.class_id, body_size=len(content), properties={'content-type': 'application/json'}, channel_number=channel_number),
            amqp_spec.Content(content=content.encode('utf8'), channel_number=channel_number)
        ]
        publish_methods = r
        for i in publish_methods:
            self.write(i)
        while 1:
            data9 = yield
            if type(data9) == amqp_spec.Heartbeat:
                self.write(amqp_spec.Heartbeat())

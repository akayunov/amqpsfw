import socket
import amqp_spec
import amqp_types
import sfw_interface
from ioloop import IOLoop

WRITE = 'write'
READ = 'read'


class Application:

    def __init__(self):
        self.socket_state = WRITE
        self.output_buffer = b''
        self.buffer_in = b''

    def parse_buffer(self):
        if len(self.buffer_in) < 7:
            return None
        else:
            frame, buffer_in = amqp_spec.decode_frame(self.buffer_in)
            self.buffer_in = buffer_in
        return frame

    def write(self, value):
        self.output_buffer = value.encoded
        IOLoop.current().modify_to_write()

    # TODO we need to devide diferent channales for diferent coroutines
    def handle_read(self):
        self.buffer_in += self.socket.recv(4096)
        frame = self.parse_buffer()
        if frame:
            self.processor_new.send(frame)

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

    def processor_new(self):

        protocol_header = sfw_interface.protocol_header([0, 0, 9, 1])
        data = yield self.write(protocol_header)
        print(data)

        channel_number = amqp_types.ShortUint(1)
        start_ok = sfw_interface.start_ok('localhost', 'PLAIN', 'root', 'privetserver', 'en_US')
        # TODO we don't need to do yield self.write because it non block operation
        data2 = yield self.write(start_ok)
        print((data2))

        tune_ok = sfw_interface.tune_ok(0, 131072, 1)
        data3 = yield self.write(tune_ok)
        print((data3))

        open = sfw_interface.conection_open('/')
        data4 = yield self.write(open)
        print((data4))

        hearbeat_frame = sfw_interface.hearbeat()
        data5 = yield self.write(hearbeat_frame)
        print((data5))

        open = sfw_interface.channel_open(channel_number)
        data5 = yield self.write(open)
        print((data5))

        flow = sfw_interface.flow(channel_number, 1)
        data6 = yield self.write(flow)
        print((data6))

        declare = sfw_interface.exchange_declare(channel_number, 'message', 'topic', 1, 1, 0, 0, 0, {})
        data7 = yield self.write(declare)
        print((data7))

        declare_q = sfw_interface.queue_declare(channel_number, 'text', 0, 1, 0, 0, 0, {})
        data8 = yield self.write(declare_q)
        print((data8))

        declare_q = sfw_interface.queue_bind(channel_number, 'text', 'message', 'text.#', 1, {})
        data8 = yield self.write(declare_q)
        print((data8))

        for i in sfw_interface.publish(channel_number, 'message', 'text.tratata', 0, 0, 'qrqwrq', {'content-type': 'application/json'}):
            data8 = yield self.write(i)
            print((data8))
        while 1:
            data9 = yield
            if type(data9) == amqp_spec.Heartbeat:
                data9 = yield self.write(sfw_interface.hearbeat())
                print((data9))

import socket
import amqp_spec
import amqp_types
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

        protocol_header = amqp_spec.ProtocolHeader(0, 0, 9, 1)
        data = yield self.write(protocol_header)
        print(data)

        channel_number = amqp_types.ShortUint(1)
        start_ok = amqp_spec.Connection.StartOk({'host': ['S', 'localhost']}, 'PLAIN', ['root', 'privetserver'], 'en_US')
        # TODO we don't need to do yield self.write because it non block operation
        data2 = yield self.write(start_ok)
        print((data2))

        tune_ok = amqp_spec.Connection.TuneOk(0, 131072, 1)
        data3 = yield self.write(tune_ok)
        print((data3))

        open = amqp_spec.Connection.Open('/', '', 0)
        data4 = yield self.write(open)
        print((data4))

        hearbeat_frame = amqp_spec.Heartbeat()
        data5 = yield self.write(hearbeat_frame)
        print((data5))

        open = amqp_spec.Channel.Open('', channel_number=channel_number)
        data5 = yield self.write(open)
        print((data5))

        flow = amqp_spec.Channel.Flow(1, channel_number=channel_number)
        data6 = yield self.write(flow)
        print((data6))

        bits = 1 * 2 ** 0 + 1 * 2 ** 1 + 0 * 2 ** 2 + 0 * 2 ** 3 + 0 * 2 ** 4
        # <<< if bit go together when pack it in one octet
        declare = amqp_spec.Exchange.Declare(1, 'message', 'topic', bits, {}, channel_number=channel_number)
        data7 = yield self.write(declare)
        print((data7))

        bits = 0 * 2 ** 0 + 1 * 2 ** 1 + 0 * 2 ** 2 + 0 * 2 ** 3 + 0 * 2 ** 4
        # <<< if bit go together when pack it in one octet
        declare_q = amqp_spec.Queue.Declare(1, 'text', bits, {}, channel_number=channel_number)
        data8 = yield self.write(declare_q)
        print((data8))

        bits = 1
        # <<< if bit go together when pack it in one octet
        declare_q = amqp_spec.Queue.Bind(1, 'text', 'message', 'text.#', bits, {}, channel_number=channel_number)
        data8 = yield self.write(declare_q)
        print((data8))








#======================== publish
        bits = 0 * 2 ** 0 + 0 * 2 ** 1
        # <<< if bit go together when pack it in one octet
        # content header
        content_string = 'qrqwrq'.encode('utf8')

        class_id = amqp_spec.Basic.Publish.class_id
        # property by order first property - highest bit 1000000000000000 - only first property
        properties_table = ['content-type']
        property_flag = 0
        property_values = []
        property = {'content-type': 'application/json'}
        for k in property:
            property_flag += 2 ** (15 - properties_table.index(k))
            property_values.append(property[k])
        r = [
            amqp_spec.Basic.Publish(1, 'message', 'text.tratata', bits, channel_number=channel_number),
            amqp_spec.Header(class_id, 0, len(content_string), property_flag, property_values, channel_number=channel_number),
            amqp_spec.Content(content_string, channel_number=channel_number)
        ]
        publish_methods = r
        for i in publish_methods:
            data8 = yield self.write(i)
            print((data8))
        while 1:
            data9 = yield
            if type(data9) == amqp_spec.Heartbeat:
                data9 = yield self.write(amqp_spec.Heartbeat())
                print((data9))

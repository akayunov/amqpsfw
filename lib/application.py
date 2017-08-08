import socket
import amqp_spec
import amqp_types
import sfw_interface
from pika import frame
from ioloop import IOLoop

WRITE = 'write'
READ = 'read'


class Application:

    def __init__(self):
        self.socket_state = WRITE
        self.output_buffer = b''
        self.buffer_in = b''

    def parse_buffer(self):
        # we read in buffer whole frame that is why we can parse it easely
        pass


    def write(self, value):
        self.output_buffer = value
        ioloop = IOLoop.current()
        ioloop.modify_to_write()

    def start(self):
        HOST = 'localhost'
        PORT = '5672'
        res = socket.getaddrinfo(HOST, PORT, socket.AF_INET, socket.SOCK_STREAM)
        af, socktype, proto, canonname, sa = res[0]
        self.socket = socket.socket(af, socktype, proto)
        self.socket.connect(sa)

        self.socket.send(sfw_interface.protocol_header([0, 0, 9, 1]).encoded)
        data = self.socket.recv(4096)
        #print(amqp_spec.ProtocolHeader.decode_frame(sfw_interface.protocol_header([0, 0, 9, 1])))
        print(amqp_spec.Frame.decode_frame(data))
        return self.socket

    def processor_new(self):
        channel_number = amqp_types.ShortUint(1)
        start_ok = sfw_interface.start_ok('localhost', 'PLAIN', 'root', 'privetserver', 'en_US')
        # #TODO remove 12414
        amqp_spec.Frame.decode_frame(start_ok.encoded)
        # #TODO remove 12414
        yield self.write(start_ok.encoded)
        data2 = self.socket.recv(4096)
        #print(frame.decode_frame(data2))
        print(amqp_spec.Frame.decode_frame(data2))


        tune_ok = sfw_interface.tune_ok(0, 131072, 5)
        yield self.write(tune_ok.encoded)
        data3 = self.socket.recv(4096)
        #print(frame.decode_frame(data3))
        print(amqp_spec.Frame.decode_frame(data3))


        open = sfw_interface.conection_open('/')
        yield self.write(open.encoded)
        data4 = self.socket.recv(4096)
        #print(frame.decode_frame(data4))
        print(amqp_spec.Frame.decode_frame(data4))

        hearbeat_frame = sfw_interface.hearbeat()
        yield self.write(hearbeat_frame.encoded)
        data5 = self.socket.recv(4096)
        #print(frame.decode_frame(data5))
        print(amqp_spec.Frame.decode_frame(data5))


        open = sfw_interface.channel_open(channel_number)
        yield self.write(open.encoded)
        data5 = self.socket.recv(4096)
        #print(frame.decode_frame(data5))
        print(amqp_spec.Frame.decode_frame(data5))


        flow = sfw_interface.flow(channel_number, 1)
        yield self.write(flow.encoded)
        data6 = self.socket.recv(4096)
        #print(frame.decode_frame(data6))
        print(amqp_spec.Frame.decode_frame(data6))


        declare = sfw_interface.exchange_declare(channel_number, 'message', 'topic', 1, 1, 0, 0, 0, {})
        yield self.write(declare.encoded)
        data7 = self.socket.recv(4096)
        #print(frame.decode_frame(data7))
        print(amqp_spec.Frame.decode_frame(data7))


        declare_q = sfw_interface.queue_declare(channel_number, 'text', 0, 1, 0, 0, 0, {})
        yield self.write(declare_q.encoded)
        data8 = self.socket.recv(4096)
        #print(frame.decode_frame(data8))
        print(amqp_spec.Frame.decode_frame(data8))


        declare_q = sfw_interface.queue_bind(channel_number, 'text', 'message', 'text.#', 1, {})
        yield self.write(declare_q.encoded)
        data8 = self.socket.recv(4096)
        #print(frame.decode_frame(data8))
        print(amqp_spec.Frame.decode_frame(data8))


        for i in sfw_interface.publish(channel_number, 'message', 'text.tratata', 0, 0, 'qrqwrq', {'content-type': 'application/json'}):
            yield self.write(i.encoded)
            data8 = self.socket.recv(4096)
            #print(frame.decode_frame(data8))
            print(amqp_spec.Frame.decode_frame(data8))

    # def processor_old(self):
    #     channel_number = amqp_types.ShortUint(1)
    #     start_ok = sfw_interface.start_ok('localhost', 'PLAIN', 'root', 'privetserver', 'en_US')
    #     self.socket.send(start_ok.encoded)
    #     data2 = self.socket.recv(4096)
    #     print(frame.decode_frame(data2))
    #
    #
    #     tune_ok = sfw_interface.tune_ok(0, 131072, 5)
    #     self.socket.send(tune_ok.encoded)
    #     #data3 = self.socket.recv(4096)
    #     #print(frame.decode_frame(data3))
    #
    #
    #     open = sfw_interface.conection_open('/')
    #     self.socket.send(open.encoded)
    #     data4 = self.socket.recv(4096)
    #     print(frame.decode_frame(data4))
    #
    #     hearbeat_frame = sfw_interface.hearbeat()
    #     self.socket.send(hearbeat_frame.encoded)
    #     data5 = self.socket.recv(4096)
    #     print(frame.decode_frame(data5))
    #
    #
    #     open = sfw_interface.channel_open(channel_number)
    #     self.socket.send(open.encoded)
    #     data5 = self.socket.recv(4096)
    #     print(frame.decode_frame(data5))
    #
    #
    #     flow = sfw_interface.flow(channel_number, 1)
    #     self.socket.send(flow.encoded)
    #     data6 = self.socket.recv(4096)
    #     print(frame.decode_frame(data6))
    #
    #
    #     declare = sfw_interface.exchange_declare(channel_number, 'message', 'topic', 1, 1, 0, 0, 0, {})
    #     self.socket.send(declare.encoded)
    #     data7 = self.socket.recv(4096)
    #     print(frame.decode_frame(data7))
    #
    #
    #     declare_q = sfw_interface.queue_declare(channel_number, 'text', 0, 1, 0, 0, 0, {})
    #     self.socket.send(declare_q.encoded)
    #     data8 = self.socket.recv(4096)
    #     print(frame.decode_frame(data8))
    #
    #
    #     declare_q = sfw_interface.queue_bind(channel_number, 'text', 'message', 'text.#', 1, {})
    #     self.socket.send(declare_q.encoded)
    #     data8 = self.socket.recv(4096)
    #     print(frame.decode_frame(data8))
    #
    #
    #     for i in sfw_interface.publish(channel_number, 'message', 'text.tratata', 0, 0, 'qrqwrq', {'content-type': 'application/json'}):
    #         self.socket.send(i.encoded)
    #         data8 = self.socket.recv(4096)
    #         print(frame.decode_frame(data8))


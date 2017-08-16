import socket
import amqp_spec
import time
import select

from collections import deque
from exceptions import SfwException

# TODO do blocking connection, my select connection, tornado connection


class Application:

    def __init__(self, ioloop):
        self.output_buffer_frames = deque()
        self.output_buffer = None
        self.buffer_in = b''
        self.host = 'localhost'
        self.port = '5672'
        self.ioloop = ioloop

    def parse_buffer(self):
        frame, buffer_in = amqp_spec.decode_frame(self.buffer_in)
        self.buffer_in = buffer_in
        return frame

    def handler(self, fd, events):
        # TODO add more events type
        if events & self.ioloop.READ:
            self.handle_read()
        elif events & self.ioloop.WRITE:
            self.handle_write()
        elif events & self.ioloop._EPOLLHUP:
            pass
        elif events & self.ioloop.ERROR:
            pass
        elif events & self.ioloop._EPOLLRDHUP:
            pass

    def modify_to_read(self):
        events = select.EPOLLIN | select.EPOLLERR | select.EPOLLPRI | select.EPOLLRDBAND | select.EPOLLHUP | select.EPOLLRDHUP
        self.ioloop.update_handler(self.fileno, events)

    def modify_to_write(self, timeout_in_seconds=None):
        events = select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP | select.EPOLLRDHUP
        self.ioloop.update_handler(self.fileno, events)
        if timeout_in_seconds:
            self.timeout_time_expired = time.time() + timeout_in_seconds

    def write(self, value, timeout_in_seconds=None):
        print('OUT:' + str(int(time.time())) + ' ' + str(value))
        self.output_buffer_frames.append(value)
        self.modify_to_write(timeout_in_seconds=timeout_in_seconds)

    # TODO we need to devide diferent channales for diferent coroutines
    def handle_read(self, by_timeout=False):
        if by_timeout:
            self.processor.send(None)
        else:
            self.buffer_in += self.socket.recv(4096)
            for frame in iter(self.parse_buffer, None):
                print('IN: ' + str(int(time.time())) + ' ' + str(frame))
                if type(frame) is amqp_spec.Heartbeat:
                    self.write(amqp_spec.Heartbeat())
                else:
                    self.processor.send(frame)

    def handle_write(self):
        # TODO use more optimize structure for slice to avoid copping
        if len(self.output_buffer_frames) > 0:
            last_frame = self.output_buffer_frames.pop()
            self.output_buffer = [last_frame.dont_wait_response, b''.join([i.encoded for i in self.output_buffer_frames]) + last_frame.encoded]
            self.output_buffer_frames = deque()
        writed_bytes = self.socket.send(self.output_buffer[1])
        self.output_buffer[1] = self.output_buffer[1][writed_bytes:]
        if not self.output_buffer[1]:
            self.modify_to_read()
            if self.output_buffer[0]:
                self.processor.send(None)
            self.output_buffer = None

    # def handle_write_old(self):
    #     # TODO use more optimize structure for slice to avoid copping
    #     if not self.output_buffer:
    #         frame = self.output_buffer_frames.popleft()
    #         # if type(frame) == amqp_spec.Header:
    #         #     import pdb;pdb.set_trace()
    #         self.output_buffer = [type(frame), frame.encoded]
    #         if type(frame) is amqp_spec.EmptyFrame:
    #             for i in list(self.output_buffer_frames):
    #                 self.output_buffer[1] += i.encoded
    #     writed_bytes = self.socket.send(self.output_buffer[1])
    #     self.output_buffer[1] = self.output_buffer[1][writed_bytes:]
    #     if not self.output_buffer[1]:
    #         # if there is another frame in byffer it mean caller don't wait responce and do some self.write(frame) without waiting response
    #         try:
    #             frame = self.output_buffer_frames.popleft()
    #         except IndexError:
    #             frame = None
    #         if frame:
    #             self.output_buffer = [type(frame), frame.encoded]
    #             # TODO try to do one more send buffer not full yet
    #         else:
    #             self.ioloop.current().modify_to_read()
    #             if self.output_buffer[0].dont_wait_response:
    #                 self.processor.send(None)
    #             self.output_buffer = None

    def sleep(self, n):
        # TODO fix it
        # flush all buffers while sleep
        self.write(amqp_spec.EmptyFrame(), timeout_in_seconds=n)

    def start(self):
        res = socket.getaddrinfo(self.host, self.port, socket.AF_INET, socket.SOCK_STREAM)
        af, socktype, proto, canonname, sa = res[0]
        self.socket = socket.socket(af, socktype, proto)
        self.socket.connect(sa)
        self.fileno = self.socket.fileno()
        protocol_header = amqp_spec.ProtocolHeader('A', 'M', 'Q', 'P', 0, 0, 9, 1)
        self.socket.send(protocol_header.encoded)
        return self.socket


    def processor(self):
        # TODO devide it more granular
        # protocol_header = amqp_spec.ProtocolHeader('A', 'M', 'Q', 'P', 0, 0, 9, 1)
        # start = yield self.write(protocol_header)
        start = yield
        channel_number = 1
        start_ok = amqp_spec.Connection.StartOk({'host': ['S', 'localhost']}, 'PLAIN', credential=['root', 'privetserver'])
        tune = yield self.write(start_ok)

        tune_ok = amqp_spec.Connection.TuneOk(heartbeat_interval=1)
        # yield self.write(tune_ok)  # it works too!!!! and frame must be send to server
        self.write(tune_ok)  # it works too!!!! and frame will be send to server on next yield

        c_open = amqp_spec.Connection.Open(virtual_host='/')
        openok = yield self.write(c_open)

        ch_open = amqp_spec.Channel.Open(channel_number=channel_number)
        ch_open_ok = yield self.write(ch_open)

        flow = amqp_spec.Channel.Flow(channel_number=channel_number)
        flow_ok = yield self.write(flow)

        ex_declare = amqp_spec.Exchange.Declare('message', channel_number=channel_number)
        declare_ok = yield self.write(ex_declare)

        declare_q = amqp_spec.Queue.Declare(queue_name='text', channel_number=channel_number)
        declare_q_ok = yield self.write(declare_q)

        bind = amqp_spec.Queue.Bind(queue_name='text', exchange_name='message', routing_key='text.#', channel_number=channel_number)
        bind_ok = yield self.write(bind)

import logging
import select
import socket
import time
from collections import deque

from amqpsfw import amqp_spec
from amqpsfw.client.configuration import Configuration

from amqpsfw.amwpsfwlogging import init_logger

# TODO do blocking connection, my select connection, tornado connection
log = logging.getLogger(__name__)
init_logger()


class Application:
    def __init__(self, ioloop):
        self.output_buffer_frames = deque()
        self.output_buffer = [0, b'']
        self.buffer_in = b''
        self.host = Configuration.host
        self.port = Configuration.port
        self.ioloop = ioloop
        self.processor = self.processor()
        self.processor.send(None)

    def parse_buffer(self):
        frame, buffer_in = amqp_spec.decode_frame(self.buffer_in)
        self.buffer_in = buffer_in
        return frame

    def handler(self, fd, event):
        # TODO add more events type
        if event & self.ioloop.READ or not event:
            self.handle_read()
        elif event & self.ioloop.WRITE:
            self.handle_write()
        elif event & self.ioloop._EPOLLHUP:
            pass
        elif event & self.ioloop.ERROR:
            pass
        elif event & self.ioloop._EPOLLRDHUP:
            pass

    def modify_to_read(self):
        events = select.EPOLLIN | select.EPOLLERR | select.EPOLLPRI | select.EPOLLRDBAND | select.EPOLLHUP | select.EPOLLRDHUP
        self.ioloop.update_handler(self.fileno, events)

    def modify_to_write(self):
        events = select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP | select.EPOLLRDHUP
        self.ioloop.update_handler(self.fileno, events)

    def write(self, value):
        log.debug('OUT:' + str(int(time.time())) + ' ' + str(value))
        self.output_buffer_frames.append(value)
        self.modify_to_write()

    # TODO we need to devide diferent channales for diferent coroutines
    def handle_read(self, by_timeout=False):
        if by_timeout:
            self.processor.send(None)
        else:
            self.buffer_in += self.socket.recv(4096)
            for frame in iter(self.parse_buffer, None):
                log.debug('IN: ' + str(int(time.time())) + ' ' + str(frame))
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
            self.output_buffer = [0, b'']

    def sleep(self, duration):
        self.modify_to_write()
        self.ioloop.current().call_later(duration, next, self.processor)
        return

    def start(self):
        res = socket.getaddrinfo(self.host, self.port, socket.AF_INET, socket.SOCK_STREAM)
        af, socktype, proto, canonname, sa = res[0]
        self.socket = socket.socket(af, socktype, proto)
        # TODO do connect non blocking
        self.socket.connect(sa)
        self.fileno = self.socket.fileno()
        protocol_header = amqp_spec.ProtocolHeader('A', 'M', 'Q', 'P', *Configuration.amqp_version)
        self.socket.send(protocol_header.encoded)
        self.socket.setblocking(0)
        return self.socket

    def processor(self):
        start = yield
        start_ok = amqp_spec.Connection.StartOk({'host': ['S', Configuration.host]}, Configuration.sals_mechanism, credential=[Configuration.credential.user, Configuration.credential.password])
        tune = yield self.write(start_ok)

        tune_ok = amqp_spec.Connection.TuneOk(heartbeat_interval=Configuration.heartbeat_interval)
        # yield self.write(tune_ok)  # it works too!!!! and frame must be send to server
        self.write(tune_ok)  # it works too!!!! and frame will be send to server on next yield

        c_open = amqp_spec.Connection.Open(virtual_host=Configuration.virtual_host)
        openok = yield self.write(c_open)

        #channel_obj = amqp_spec.Channel()
        #ch_open = channel_obj.Open(channel_number=1)
        ch_open1 = amqp_spec.Channel.Open(channel_number=1)
        ch_open_ok = yield self.write(ch_open1)

        ch_open2 = amqp_spec.Channel.Open(channel_number=2)
        ch_open_ok = yield self.write(ch_open2)

        flow = amqp_spec.Channel.Flow(channel_number=ch_open1.channel_number)
        flow_ok = yield self.write(flow)

        ex_declare = amqp_spec.Exchange.Declare('message', channel_number=ch_open1.channel_number)
        declare_ok = yield self.write(ex_declare)

        declare_q = amqp_spec.Queue.Declare(queue_name='text', channel_number=ch_open1.channel_number)
        declare_q_ok = yield self.write(declare_q)

        bind = amqp_spec.Queue.Bind(queue_name='text', exchange_name='message', routing_key='text.#', channel_number=ch_open1.channel_number)
        bind_ok = yield self.write(bind)


        flow = amqp_spec.Channel.Flow(channel_number=ch_open2.channel_number)
        flow_ok = yield self.write(flow)

        ex_declare = amqp_spec.Exchange.Declare('message', channel_number=ch_open2.channel_number)
        declare_ok = yield self.write(ex_declare)

        declare_q = amqp_spec.Queue.Declare(queue_name='text', channel_number=ch_open2.channel_number)
        declare_q_ok = yield self.write(declare_q)

        bind = amqp_spec.Queue.Bind(queue_name='text', exchange_name='message', routing_key='text.#', channel_number=ch_open2.channel_number)
        bind_ok = yield self.write(bind)
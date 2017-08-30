import logging
import select
import socket
import time
from collections import deque

from amqpsfw import amqp_spec
from amqpsfw.client.configuration import Configuration

from amqpsfw.logger import init_logger

# TODO do blocking connection, my select connection, tornado connection
log = logging.getLogger(__name__)
init_logger()


class Application:
    def __init__(self, ioloop):
        # TODO too many buffers easy to confuse
        self.output_buffer_frames = deque()
        self.output_buffer = [0, b'']
        self.buffer_in = b''
        self.host = Configuration.host
        self.port = Configuration.port
        self.ioloop = ioloop
        self.processor = self.processor()
        res = socket.getaddrinfo(self.host, self.port, socket.AF_INET, socket.SOCK_STREAM)
        af, socktype, proto, canonname, sa = res[0]
        self.socket = socket.socket(af, socktype, proto)
        # TODO do connect non blocking
        self.socket.connect(sa)
        self.fileno = self.socket.fileno()
        protocol_header = amqp_spec.ProtocolHeader('A', 'M', 'Q', 'P', *Configuration.amqp_version)
        self.socket.send(protocol_header.encoded)
        self.socket.setblocking(0)
        self.ioloop.add_handler(self.socket.fileno(), self.handler, ioloop.READ)
        self.processor.send(None)

    def parse_buffer(self):
        frame, buffer_in = amqp_spec.decode_frame(self.buffer_in)
        self.buffer_in = buffer_in
        return frame

    def handler(self, fd, event):
        # TODO add more events type
        if event & self.ioloop.READ or not event:
            self.handle_read()
        if event & self.ioloop.WRITE:
            self.handle_write()
        # TODO fix it
        # if event & self.ioloop._EPOLLHUP:
        #     pass
        # if event & self.ioloop.ERROR:
        #     pass
        # if event & self.ioloop._EPOLLRDHUP:
        #     pass

    def modify_to_read(self):
        events = select.EPOLLIN | select.EPOLLERR | select.EPOLLPRI | select.EPOLLRDBAND | select.EPOLLHUP | select.EPOLLRDHUP
        self.ioloop.update_handler(self.fileno, events)

    def modify_to_write(self):
        # TODO EPOLLIN - tests it
        events = select.EPOLLOUT | select.EPOLLIN | select.EPOLLERR | select.EPOLLHUP | select.EPOLLRDHUP
        self.ioloop.update_handler(self.fileno, events)

    def write(self, value):
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
                response = self.method_handler(frame)
                if response:
                    self.processor.send(response)

    def handle_write(self):
        # TODO use more optimize structure for slice to avoid copping
        if len(self.output_buffer_frames) > 0 and not self.output_buffer[1]:
            last_frame = self.output_buffer_frames.pop()
            self.output_buffer = [last_frame.dont_wait_response, b''.join([i.encoded for i in self.output_buffer_frames]) + last_frame.encoded]
            self.output_buffer_frames = deque()
            log.debug('OUT:' + str(int(time.time())) + ' ' + str(last_frame))
        if self.output_buffer[1]:
            writed_bytes = self.socket.send(self.output_buffer[1])
            self.output_buffer[1] = self.output_buffer[1][writed_bytes:]
        if not self.output_buffer[1] and not len(self.output_buffer_frames):
            self.modify_to_read()
            # TODO move it on namedtuple
            if self.output_buffer[0]:
                self.processor.send(None)
            self.output_buffer = [0, b'']

    def sleep(self, duration):
        self.modify_to_write()
        self.ioloop.current().call_later(duration, next, self.processor)
        return

    def processor(self):
        yield

    def stop(self):
        # TODO flush buffers before ioloop stop
        self.buffer_in = b''
        self.output_buffer_frames = deque()
        self.output_buffer = [0, b'']
        self.ioloop.stop()
        # TODO fix it - uncomment and get error on handle_write because in handle we put in second branch on write event
        # self.socket.close()

    def on_hearbeat(self, method):
        self.write(amqp_spec.Heartbeat())

    def on_connection_start(self, method):
        self.write(amqp_spec.Connection.StartOk({'host': Configuration.host}, Configuration.sals_mechanism, credential=[Configuration.credential.user, Configuration.credential.password]))

    def on_connection_tune(self, method):
        self.write(amqp_spec.Connection.TuneOk(heartbeat_interval=Configuration.heartbeat_interval))

    def on_connection_secure(self, method):
        self.write(amqp_spec.Connection.SecureOk(response='tratata'))

    def on_connection_close(self, method):
        start_ok = amqp_spec.Connection.CloseOk()
        self.write(start_ok)
        # TODO fix it
        # self.stop()

    def on_channel_flow(self, method):
        self.write(amqp_spec.Channel.FlowOk())

    def on_channel_close(self, method):
        self.write(amqp_spec.Channel.CloseOk())

    method_mapper = {
        amqp_spec.Heartbeat: on_hearbeat,
        amqp_spec.Connection.Start: on_connection_start,
        amqp_spec.Connection.Tune: on_connection_tune,
        amqp_spec.Connection.Secure: on_connection_secure,
        amqp_spec.Connection.Close: on_connection_close,
        amqp_spec.Channel.Flow: on_channel_flow,
        amqp_spec.Channel.Close: on_channel_close
    }

    def method_handler(self, method):
        if type(method) in self.method_mapper:
            return self.method_mapper[type(method)](self, method)
        else:
            return method

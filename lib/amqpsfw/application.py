import logging
import select

import time
from collections import deque

from amqpsfw import amqp_spec
from amqpsfw.client.configuration import Configuration
from amqpsfw.logger import init_logger
from amqpsfw.exceptions import SfwException


log = logging.getLogger(__name__)
init_logger()


class Application:
    STOPPED = 'STOPPPED'
    RUNNING = 'RUNNING'

    def __init__(self, ioloop):
        # TODO too many buffers easy to confuse
        self.output_buffer_frames = deque()
        self.output_buffer = [0, b'']
        self.buffer_in = {'bytes': b'', 'need_to_read': 0}
        self.host = Configuration.host
        self.port = Configuration.port
        self.ioloop = ioloop
        self.status = 'RUNNING'
        self.start()

    def start(self):
        raise NotImplementedError

    def handler(self, fd, event):
        # TODO add more events type
        if event & self.ioloop.READ and self.status == 'RUNNING':
            self.handle_read()
        if event & self.ioloop.WRITE and self.status == 'RUNNING':
            self.handle_write()
        if event & self.ioloop.ERROR:
            self.handle_error()
        # TODO fix it
        # if event & self.ioloop._EPOLLHUP:
        #     pass
        # if event & self.ioloop.ERROR:
        #     pass
        # if event & self.ioloop._EPOLLRDHUP:
        #     pass
        # if event & (select.EPOLLIN | select.EPOLLPRI | select.EPOLLRDBAND):
        #     log.debug('IN: %s %s %s', str(int(time.time())), events, next_timeout_callback)
        #     self.handler(self.fileno, event)
        # if event & select.EPOLLOUT:
        #     log.debug('OUT: %s %s %s', str(int(time.time())), events, next_timeout_callback)
        #     self.handler(self.fileno, event)
        # if event & select.EPOLLHUP:
        #     pass
        # if event & select.EPOLLERR:
        #     pass
        # if event & select.EPOLLRDHUP:
        #     pass

    def modify_to_read(self):
        events = select.EPOLLIN | select.EPOLLERR | select.EPOLLPRI | select.EPOLLRDBAND | select.EPOLLHUP | select.EPOLLRDHUP
        self.ioloop.update_handler(self.fileno, events)

    def modify_to_write(self):
        events = select.EPOLLOUT | select.EPOLLIN | select.EPOLLERR | select.EPOLLHUP | select.EPOLLRDHUP
        self.ioloop.update_handler(self.fileno, events)

    def write(self, value):
        self.output_buffer_frames.append(value)
        self.modify_to_write()

    def handle_error(self):
        raise SfwException('Internal', 'Socket error in handle error')

    def handle_read(self):
        # TODO if many dat ain buffer then we will be run this cycle while buffe became empty but in case Basic.Ack we need to write it immediatly
        # TODO try to read only one frame in time but it's inefficient
        # TODO so I need to rethink this handle_read and hanle_write
        if not self.buffer_in['bytes']:
            self.buffer_in['bytes'] += self.socket.recv(8)
        else:
            self.buffer_in['bytes'] += self.socket.recv(self.buffer_in['need_to_read'])
        payload_size, frame, _ = amqp_spec.decode_frame(self.buffer_in['bytes'])
        # TODO handle read known about frame structure - non good for him
        self.buffer_in['need_to_read'] = payload_size - (len(self.buffer_in['bytes']) - 8)
        if frame:
            self.buffer_in['bytes'] = b''
            self.buffer_in['need_to_read'] = 0
            log.debug('IN: ' + str(int(time.time())) + ' ' + str(frame))
            response = self.method_handler(frame)
            if response:
                # TODO why this try here?
                try:
                    self.processor.send(response)
                except StopIteration:
                    pass

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
                # TODO why this try here?
                try:
                    self.processor.send(None)
                except StopIteration:
                    pass
            self.output_buffer = [0, b'']

    def sleep(self, duration):
        self.modify_to_write()
        self.ioloop.current().call_later(duration, next, self.processor)
        return

    def processor(self):
        raise NotImplementedError

    def stop(self):
        self.buffer_in['bytes'] = b''
        self.buffer_in['need_to_read'] = 0
        self.output_buffer_frames = deque()
        self.output_buffer = [0, b'']
        self.status = self.STOPPED
        self.ioloop.stop()
        self.socket.close()

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
        self.stop()

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

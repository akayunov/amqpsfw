import logging
import select
import socket

import time
from collections import deque

from amqpsfw import amqp_spec
from amqpsfw.exceptions import SfwException


amqpsfw_logger = logging.getLogger('amqpsfw')
log_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(module)s.py.%(funcName)s:%(lineno)d - %(message)s')
log_handler.setFormatter(formatter)
amqpsfw_logger.addHandler(log_handler)
amqpsfw_logger.setLevel(logging.DEBUG)


log = logging.getLogger(__name__)


class OutputBuffer:
    def __init__(self):
        self.frame_queue = deque()
        self.current_frame_bytes = b''
        self.dont_wait_response = 0

    def clear(self):
        self.current_frame_bytes = b''
        self.dont_wait_response = 0

    def append_frame(self, x):
        self.frame_queue.append(x)


class Application:
    STOPPED = 'STOPPPED'
    RUNNING = 'RUNNING'

    def __init__(self, ioloop):
        self.output_buffer = OutputBuffer()
        self.buffer_in = b''
        self.ioloop = ioloop
        self.status = 'RUNNING'
        self.start()

    def set_config(self, config):
        self.config = config

    def start(self):
        raise NotImplementedError

    def handler(self, fd, event):
        # TODO add more events type
        if event & self.ioloop.WRITE and self.status == 'RUNNING':
            self.handle_write()
        if event & self.ioloop.READ and self.status == 'RUNNING':
            self.handle_read()
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
        self.output_buffer.append_frame(value)
        self.modify_to_write()

    def handle_error(self):
        raise SfwException('Internal', 'Socket error in handle error')

    def handle_read(self):
        # we cant parse full buffer in one call because if many data in buffer then we will be run this cycle by buffer while buffer became empty
        # but in case Basic.Ack we need to write response immediatly after get frame
        # try to read all data and stay non parse yet in buffer to get read event from pool again,
        # after we've parsed frame - read again to remove frame from socket
        # TODO but now we read frame by frame but can read all buffer at one attempt, but it simple
        payload_size, frame, self.buffer_in = amqp_spec.decode_frame(self.buffer_in)
        if not frame:
            self.buffer_in = self.socket.recv(4096, socket.MSG_PEEK)
            if not self.buffer_in:
                self.stop()
            payload_size, frame, self.buffer_in = amqp_spec.decode_frame(self.buffer_in)
        if frame:
            # remove already parsed data, do second read without flag
            # TODO do it by one read on all frame from buffer to performance
            self.socket.recv(payload_size + 8)
            log.debug(frame)
            response = self.method_handler(frame)
            if response:
                # TODO why this try here?
                try:
                    self.processor.send(response)
                except StopIteration:
                    pass

    def handle_write(self):
        # TODO use more optimize structure for slice to avoid copping
        if len(self.output_buffer.frame_queue) > 0 and not self.output_buffer.current_frame_bytes:
            self.output_buffer.dont_wait_response = self.output_buffer.frame_queue[-1].dont_wait_response
            for frame in self.output_buffer.frame_queue:
                log.debug('OUT:' + str(frame))
                self.output_buffer.current_frame_bytes += frame.encoded
            self.output_buffer.frame_queue.clear()
        if self.output_buffer.current_frame_bytes:
            writed_bytes = self.socket.send(self.output_buffer.current_frame_bytes)
            self.output_buffer.current_frame_bytes = self.output_buffer.current_frame_bytes[writed_bytes:]
        if not self.output_buffer.current_frame_bytes and not len(self.output_buffer.frame_queue):
            self.modify_to_read()
            if self.output_buffer.dont_wait_response:
                # TODO why this try here?
                try:
                    self.processor.send(None)
                except StopIteration:
                    pass
            self.output_buffer.clear()

    def sleep(self, duration):
        self.modify_to_write()
        self.ioloop.current().call_later(duration, next, self.processor)
        return

    def processor(self):
        raise NotImplementedError

    def stop(self):
        log.debug('Stop application')
        self.buffer_in = b''
        self.output_buffer.clear()
        self.status = self.STOPPED
        self.ioloop.stop()
        self.socket.close()

    def on_hearbeat(self, method):
        self.write(amqp_spec.Heartbeat())

    def on_connection_start(self, method):
        self.write(amqp_spec.Connection.StartOk({'host': self.config.host}, self.config.sals_mechanism, credential=[self.config.credential.user, self.config.credential.password]))
    def on_connection_tune(self, method):
        self.write(amqp_spec.Connection.TuneOk(heartbeat_interval=self.config.heartbeat_interval))

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

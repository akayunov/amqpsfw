import logging
import select
import socket

from collections import deque

from amqpsfw import amqp_spec
from amqpsfw.exceptions import SfwException
from amqpsfw.configuration import Configuration

amqpsfw_logger = logging.getLogger('amqpsfw')
log_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(module)s.py.%(funcName)s:%(lineno)d - %(message)s')
log_handler.setFormatter(formatter)
amqpsfw_logger.addHandler(log_handler)
amqpsfw_logger.setLevel(logging.ERROR)

log = logging.getLogger(__name__)


class BufferOut:
    def __init__(self):
        self.frame_queue = deque()
        self.current_frame_bytes = b''

    def clear(self):
        self.current_frame_bytes = b''

    def append_frame(self, x):
        self.frame_queue.append(x)


class BufferIn:
    def __init__(self):
        self.frame_bytes = b''
        self.parsed_data_size = 0

    def clear(self):
        self.__init__()


class Application:
    STOPPED = 'STOPPPED'
    RUNNING = 'RUNNING'
    READ = select.EPOLLIN
    WRITE = select.EPOLLOUT
    ERROR = (select.EPOLLERR | select.EPOLLPRI | select.EPOLLHUP | select.EPOLLRDHUP | select.EPOLLRDBAND)
    PRIWRITE = select.EPOLLWRBAND  # doesn't use

    def __init__(self, ioloop, app_socket=None):
        self.buffer_out = BufferOut()
        self.buffer_in = BufferIn()
        self.ioloop = ioloop
        self.status = 'RUNNING'
        self.socket = app_socket
        self.expected_response_frames = []
        self.app_gen = self.processor()
        self.config = Configuration()

    def start(self):
        raise NotImplementedError

    def modify_to_read(self):
        events = self.READ | self.ERROR
        self.ioloop.update_handler(self.socket.fileno(), events)

    def modify_to_write(self):
        events = self.WRITE | self.READ | self.ERROR
        self.ioloop.update_handler(self.socket.fileno(), events)

    def write(self, value):
        if self.status == self.STOPPED:
            raise SfwException('Internal', 'Aplication is stopped')
        self.buffer_out.append_frame(value)
        self.modify_to_write()

    def handler(self, fd, event):
        # TODO why == RUNNING is here?
        if event & self.WRITE and self.status == 'RUNNING':
            self.handle_write()
        if event & self.READ and self.status == 'RUNNING':
            self.handle_read()
        if event & self.ERROR:
            self.handle_error(fd)

    def handle_error(self, fd):
        log.error('Get error on socket: %s', fd)
        self.stop()

    def handle_read(self):
        # we cant parse full buffer in one call because if many data in buffer then we will be run this cycle by buffer while buffer became empty
        # but in case Basic.Ack we need to write response immediatly after get frame,
        # so we read data, but don't remove it from socket buffer for getting read events again and then all data in app buffer is parser
        #  remove data from socket buffer
        payload_size, frame, self.buffer_in.frame_bytes = amqp_spec.decode_frame(self.buffer_in.frame_bytes)
        if not frame:
            self.buffer_in.frame_bytes = self.socket.recv(4096, socket.MSG_PEEK)
            if not self.buffer_in.frame_bytes:
                self.stop()
            payload_size, frame, self.buffer_in.frame_bytes = amqp_spec.decode_frame(self.buffer_in.frame_bytes)
        if frame:
            self.buffer_in.parsed_data_size += (payload_size + 8)
            log.debug('IN {}: {}'.format(self.socket.fileno(), frame))
            if self.expected_response_frames and not issubclass(type(frame), tuple(self.expected_response_frames)):
                log.error('Unexpected frame type: %s', str(frame))
                self.stop()
            else:
                self.expected_response_frames = []
            response = self.method_handler(frame)
            _, next_frame, _ = amqp_spec.decode_frame(self.buffer_in.frame_bytes)
            if not next_frame:
                # "frame" was last frame in buffer_in so remove already parsed data, do second read without flag
                self.socket.recv(self.buffer_in.parsed_data_size)
                self.buffer_in.clear()
            if response:
                # TODO why this try here?
                try:
                    self.app_gen.send(response)
                except StopIteration:
                    pass

    def handle_write(self):
        if len(self.buffer_out.frame_queue) > 0 and not self.buffer_out.current_frame_bytes:
            self.expected_response_frames = self.buffer_out.frame_queue[-1].expected_response_frames
            for frame in self.buffer_out.frame_queue:
                log.debug('OUT {}: {}'.format(self.socket.fileno(), frame))
                self.buffer_out.current_frame_bytes += frame.encoded
            self.buffer_out.frame_queue.clear()
        if self.buffer_out.current_frame_bytes:
            writed_bytes = self.socket.send(self.buffer_out.current_frame_bytes)
            self.buffer_out.current_frame_bytes = self.buffer_out.current_frame_bytes[writed_bytes:]
        if not self.buffer_out.current_frame_bytes and not len(self.buffer_out.frame_queue):
            self.modify_to_read()
            if self.expected_response_frames is None:
                # TODO why this try here?
                try:
                    self.app_gen.send(None)
                except StopIteration:
                    pass
            self.buffer_out.clear()

    def sleep(self, duration):
        self.modify_to_write()
        self.ioloop.current().call_later(duration, next, self.app_gen)
        return

    def processor(self):
        yield
        raise NotImplementedError

    def stop(self):
        log.debug('Stop application')
        self.buffer_in.clear()
        self.buffer_out.clear()
        self.status = self.STOPPED
        self.ioloop.stop()
        self.socket.close()

    def on_hearbeat(self, method):
        self.write(amqp_spec.Heartbeat())

    def on_connection_close(self, method):
        self.write(amqp_spec.Connection.CloseOk())
        self.stop()

    def on_channel_flow(self, method):
        # TODO if active=0 stop sending data
        self.write(amqp_spec.Channel.FlowOk(method.active))

    def on_channel_close(self, method):
        self.write(amqp_spec.Channel.CloseOk())

    method_mapper = {
        amqp_spec.Heartbeat: on_hearbeat,
        amqp_spec.Connection.Close: on_connection_close,
        amqp_spec.Channel.Close: on_channel_close,
        amqp_spec.Channel.Flow: on_channel_flow
    }

    def method_handler(self, method):
        if type(method) in self.method_mapper:
            return self.method_mapper[type(method)](self, method)
        else:
            return method

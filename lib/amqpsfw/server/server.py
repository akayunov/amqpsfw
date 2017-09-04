from collections import deque
import logging
import socket
from amqpsfw import amqp_spec
from amqpsfw.application import Application
from amqpsfw.server.configuration import Configuration
from amqpsfw.logger import init_logger

log = logging.getLogger(__name__)
init_logger()

cc = None

class ServerClient(Application):
    def __init__(self, ioloop, socket=None):
        # TODO too many buffers easy to confuse
        self.output_buffer_frames = deque()
        self.output_buffer = [0, b'']
        self.buffer_in = b''
        self.ioloop = ioloop
        self.status = 'RUNNING'
        self.socket = socket
        self.start()

    def start(self):
        self.set_config(Configuration)
        self.processor = self.processor()
        res = socket.getaddrinfo(self.config.host, self.config.port, socket.AF_INET, socket.SOCK_STREAM)
        af, socktype, proto, canonname, sa = res[0]
        # self.socket = socket.socket(af, socktype, proto)
        self.fileno = self.socket.fileno()
        self.ioloop.add_handler(self.socket.fileno(), self.handler, self.ioloop.WRITE)
        self.socket.setblocking(0)
        # try:
        #     self.socket.connect(sa)
        # except BlockingIOError as e:
        #     if e.errno == 115:
        #         pass
        #     else:
        #         raise
        self.processor.send(None)

    def processor(self):
        while 1:
            protocol_header = yield
            yield self.write(amqp_spec.Connection.Start(0, 9,{},Configuration.sals_mechanism))
            print(protocol_header)


class Server(Application):
    def connection_accept(self, fd, event):
        # TODO add more events type
        if event & self.ioloop.READ and self.status == 'RUNNING':
            global cc
            client_socket, addr = self.socket.accept()
            cc = client_socket
            log.debug('CLIENT SOCKET: ' + str(client_socket.fileno()) + str(addr))
            s = ServerClient(self.ioloop, client_socket)
        if event & self.ioloop.WRITE and self.status == 'RUNNING':
            self.handle_write()
        if event & self.ioloop.ERROR:
            self.handle_error()

    def start(self):
        self.set_config(Configuration)
        self.processor = self.processor()
        res = socket.getaddrinfo(self.config.host, self.config.port, socket.AF_INET, socket.SOCK_STREAM)
        af, socktype, proto, canonname, sa = res[0]
        self.socket = socket.socket(af, socktype, proto)
        self.fileno = self.socket.fileno()
        log.debug('SERVER SOCKET: ' + str(self.fileno))
        self.ioloop.add_handler(self.socket.fileno(), self.connection_accept, self.ioloop.READ)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.setblocking(0)
        # try:
        #     self.socket.bind(sa)
        #     self.socket.listen(10)
        # except BlockingIOError as e:
        #     if e.errno == 115:
        #         pass
        #     else:
        #         raise
        self.socket.bind(sa)
        self.socket.listen(10)
        self.processor.send(None)

    def processor(self):
        yield

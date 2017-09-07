import logging
import socket

from amqpsfw.application import Application

log = logging.getLogger(__name__)

cc = None


class ServerClient(Application):
    def start(self):
        self.app_gen = self.processor()
        # res = socket.getaddrinfo(self.config.host, self.config.port, socket.AF_INET, socket.SOCK_STREAM)
        # af, socktype, proto, canonname, sa = res[0]
        # self.socket = socket.socket(af, socktype, proto)
        self.fileno = self.socket.fileno()
        self.ioloop.add_handler(self.socket.fileno(), self.handler, self.WRITE | self.ERROR)
        self.socket.setblocking(0)
        # try:
        #     self.socket.connect(sa)
        # except BlockingIOError as e:
        #     if e.errno == 115:
        #         pass
        #     else:
        #         raise
        self.app_gen.send(None)

    def processor(self):
        yield

class Server(Application):
    def __init__(self, ioloop, server_aplication):
        self.connection_application = server_aplication
        super().__init__(ioloop)

    def connection_accept(self, fd, event):
        # TODO add more events type
        if event & self.READ and self.status == 'RUNNING':
            global cc
            client_socket, addr = self.socket.accept()
            cc = client_socket
            log.debug('CLIENT SOCKET ON SERVER SIDE: ' + str(client_socket.fileno()) + str(addr))
            s = self.connection_application(self.ioloop, client_socket)
            s.start()
        if event & self.WRITE and self.status == 'RUNNING':
            self.handle_write()
        if event & self.ERROR:
            self.handle_error(fd)

    def start(self):
        self.app_gen = self.processor()
        res = socket.getaddrinfo(self.config.host, self.config.port, socket.AF_INET, socket.SOCK_STREAM)
        af, socktype, proto, canonname, sa = res[0]
        self.socket = socket.socket(af, socktype, proto)
        self.fileno = self.socket.fileno()
        log.debug('SERVER SOCKET: ' + str(self.fileno))
        self.ioloop.add_handler(self.socket.fileno(), self.connection_accept, self.READ | self.ERROR)
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
        self.app_gen.send(None)

    def processor(self):
        protocol_header = yield

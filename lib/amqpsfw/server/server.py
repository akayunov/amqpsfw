import logging
import socket

from amqpsfw.application import Application

log = logging.getLogger(__name__)


class Server(Application):
    def __init__(self, ioloop, server_aplication_class):
        self.server_aplication_class = server_aplication_class
        super().__init__(ioloop)

    def start(self):
        res = socket.getaddrinfo(self.config.host, self.config.port, socket.AF_INET, socket.SOCK_STREAM)
        af, socktype, proto, canonname, sa = res[0]
        self.socket = socket.socket(af, socktype, proto)
        log.debug('Servr socket: %s', self.socket.fileno())
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.setblocking(0)
        self.ioloop.add_handler(self.socket.fileno(), self.accept, self.READ | self.ERROR)
        self.socket.bind(sa)
        self.socket.listen(self.config.listen_queue_size)

    def accept(self, fd, event):
        if event & self.READ and self.status == 'RUNNING':
            client_socket, addr = self.socket.accept()
            log.debug('Client socket on server side: %s %s', client_socket.fileno(), addr)
            server_application = self.server_aplication_class(self.ioloop, client_socket)
            server_application.start()
        else:
            self.handle_error(fd)

    def processor(self):
        yield


class ServerClient(Application):
    def start(self):
        self.ioloop.add_handler(self.socket.fileno(), self.handler, self.WRITE | self.ERROR)
        self.socket.setblocking(0)
        self.app_gen.send(None)

    def processor(self):
        yield

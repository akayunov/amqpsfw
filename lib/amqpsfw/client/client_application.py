import socket

from amqpsfw.application import Application
from amqpsfw import amqp_spec
from amqpsfw.client.configuration import Configuration
from amqpsfw import ioloop


class Client(Application):
    def start(self):
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
        self.ioloop.add_handler(self.socket.fileno(), self.handler, self.ioloop.READ)
        self.processor.send(None)
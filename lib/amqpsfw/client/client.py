import socket

from amqpsfw.application import Application
from amqpsfw import amqp_spec
from amqpsfw.client.configuration import Configuration


class Client(Application):
    def start(self):
        self.set_config(Configuration)
        self.processor = self.processor()
        res = socket.getaddrinfo(self.config.host, self.config.port, socket.AF_INET, socket.SOCK_STREAM)
        af, socktype, proto, canonname, sa = res[0]
        self.socket = socket.socket(af, socktype, proto)
        self.fileno = self.socket.fileno()
        self.ioloop.add_handler(self.socket.fileno(), self.handler, self.ioloop.WRITE)
        self.socket.setblocking(0)
        try:
            self.socket.connect(sa)
        except BlockingIOError as e:
            if e.errno == 115:
                pass
            else:
                raise
        self.processor.send(None)

    def processor(self):
        protocol_header = amqp_spec.ProtocolHeader('A', 'M', 'Q', 'P', *Configuration.amqp_version)
        yield self.write(protocol_header)

import logging
import socket
from amqpsfw.application import Application
from amqpsfw import amqp_spec

log = logging.getLogger(__name__)


class Client(Application):
    def start(self):
        res = socket.getaddrinfo(self.config.host, self.config.port, socket.AF_INET, socket.SOCK_STREAM)
        af, socktype, proto, canonname, sa = res[0]
        self.socket = socket.socket(af, socktype, proto)
        log.debug('Client socket on client side: ' + str(self.socket.fileno()) + str(sa))
        self.ioloop.add_handler(self.socket.fileno(), self.handler, self.WRITE | self.ERROR)
        self.socket.setblocking(0)
        try:
            self.socket.connect(sa)
        except BlockingIOError as e:
            if e.errno == 115:
                pass
            else:
                raise
        self.app_gen.send(None)

    def processor(self):
        connection_start = yield self.write(amqp_spec.ProtocolHeader('A', 'M', 'Q', 'P', *self.config.amqp_version))
        # TODO server can answer rigth protocol_header id client send wrong one
        self.config.server_properties = connection_start.server_properties
        self.config.server_version_major = connection_start.version_major
        self.config.server_version_minor = connection_start.version_minor
        self.config.server_security_mechanisms = connection_start.mechanisms
        self.config.server_locale = connection_start.locale
        frame = yield self.write(amqp_spec.Connection.StartOk(
            self.config.client_properties, self.config.security_mechanisms, credential=[self.config.credential.user, self.config.credential.password]
        ))
        if type(frame) is amqp_spec.Connection.Secure:
            self.config.secure_challenge = frame.challenge
            tune = yield self.write(amqp_spec.Connection.SecureOk(response=self.config.secure_response))
        else:
            tune = frame
        self.config.channel_max = tune.channel_max
        self.config.frame_max = tune.frame_max
        self.config.heartbeat_interval = tune.heartbeat_interval
        yield self.write(amqp_spec.Connection.TuneOk(heartbeat_interval=self.config.heartbeat_interval))
        yield self.write(amqp_spec.Connection.ConOpen(virtual_host=self.config.virtual_host))

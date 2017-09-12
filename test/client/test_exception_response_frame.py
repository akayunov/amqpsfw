import os.path
import sys
import pytest

sys.path = [os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib')] + sys.path

from amqpsfw import amqp_spec, ioloop
from amqpsfw.client.client import Client
from amqpsfw.exceptions import SfwException


class TestClientException:
    def test_client_exception(self):
        class PublishAplication(Client):
            def processor(self):
                connection_start = yield self.write(amqp_spec.ProtocolHeader('A', 'M', 'Q', 'P', 30, 30, 23, 31))
                with pytest.raises(SfwException, match='Internal - Aplication is stopped'):
                    frame = yield self.write(amqp_spec.Connection.StartOk(
                        self.config.client_properties, self.config.security_mechanisms, credential=[self.config.credential.user, self.config.credential.password]
                    ))

        def start_aplication():
            io_loop = ioloop.IOLoop()
            app = PublishAplication(io_loop)
            app.start()
            io_loop.start()

        start_aplication()

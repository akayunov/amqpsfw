import os.path
import sys

sys.path = [os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib')] + sys.path

from amqpsfw import amqp_spec, ioloop
from amqpsfw.server.server import Server, ServerClient
from amqpsfw.client.client import Client
from amqpsfw.client.configuration import ClientConfiguration
from amqpsfw.server.configuration import ServerConfiguration


class TestServer:
    def test_server_basic(self):
        class ClientPublishAplication(Client):

            def processor(self):
                channel_number = 1
                start = yield from super().processor()
                ch_open1 = amqp_spec.Channel.Open(channel_number=1)
                ch_open_ok = yield self.write(ch_open1)


                flow = amqp_spec.Channel.Flow(channel_number=ch_open1.channel_number)
                flow_ok = yield self.write(flow)

                ex_declare = amqp_spec.Exchange.Declare('message', channel_number=ch_open1.channel_number)
                declare_ok = yield self.write(ex_declare)

                declare_q = amqp_spec.Queue.Declare(queue_name='text', channel_number=ch_open1.channel_number)
                declare_q_ok = yield self.write(declare_q)

                bind = amqp_spec.Queue.Bind(queue_name='text', exchange_name='message', routing_key='text.#', channel_number=ch_open1.channel_number)
                bind_ok = yield self.write(bind)

                yield self.sleep(3)
                for t in range(100):
                    content = "qwe" + str(t)
                    response = yield self.write(amqp_spec.Basic.Publish(exchange_name='message', routing_key='text.tratata', channel_number=channel_number))
                    assert response is None
                    response = yield self.write(amqp_spec.Header(class_id=amqp_spec.Basic.Publish.class_id, body_size=len(content), header_properties={'content-type': 'application/json'}, channel_number=channel_number))
                    assert response is None
                    response = yield self.write(amqp_spec.Content(content=content, channel_number=channel_number))
                    assert response is None
                response = yield self.write(amqp_spec.Channel.Close(channel_number=channel_number))
                assert type(response) is amqp_spec.Channel.CloseOk
                response = yield self.write(amqp_spec.Connection.Close())
                assert type(response) is amqp_spec.Connection.CloseOk
                yield self.stop()

        io_loop = ioloop.IOLoop()
        s_app = Server(io_loop)
        c_app = ClientPublishAplication(io_loop)
        c_app.config = ClientConfiguration()
        s_app.config = ServerConfiguration()
        c_app.config.port = 55555
        s_app.config.port = 55555
        s_app.start()
        c_app.start()
        io_loop.start()

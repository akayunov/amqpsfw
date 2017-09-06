import os.path
import sys

sys.path = [os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib')] + sys.path

from amqpsfw import amqp_spec
from amqpsfw.client.client import Client
from amqpsfw.client.configuration import Configuration
from tornado import ioloop


class TestClientPublishTornado:
    def test_client_publish_tornado(self):
        class TornadoAplication(Client):
            method_mapper = {}

            def processor(self):
                channel_number = 1
                start = yield from super().processor()
                start_ok = amqp_spec.Connection.StartOk({'host': Configuration.host}, Configuration.sals_mechanism, credential=[Configuration.credential.user, Configuration.credential.password])
                tune = yield self.write(start_ok)

                tune_ok = amqp_spec.Connection.TuneOk(heartbeat_interval=Configuration.heartbeat_interval)
                # yield self.write(tune_ok)  # it works too!!!! and frame must be send to server
                self.write(tune_ok)  # it works too!!!! and frame will be send to server on next yield

                c_open = amqp_spec.Connection.Open(virtual_host=Configuration.virtual_host)
                openok = yield self.write(c_open)

                # channel_obj = amqp_spec.Channel()
                # ch_open = channel_obj.Open(channel_number=1)
                ch_open1 = amqp_spec.Channel.Open(channel_number=1)
                ch_open_ok = yield self.write(ch_open1)

                ch_open2 = amqp_spec.Channel.Open(channel_number=2)
                ch_open_ok = yield self.write(ch_open2)

                flow = amqp_spec.Channel.Flow(channel_number=ch_open1.channel_number)
                flow_ok = yield self.write(flow)

                ex_declare = amqp_spec.Exchange.Declare('message', channel_number=ch_open1.channel_number)
                declare_ok = yield self.write(ex_declare)

                declare_q = amqp_spec.Queue.Declare(queue_name='text', channel_number=ch_open1.channel_number)
                declare_q_ok = yield self.write(declare_q)

                bind = amqp_spec.Queue.Bind(queue_name='text', exchange_name='message', routing_key='text.#', channel_number=ch_open1.channel_number)
                bind_ok = yield self.write(bind)

                flow = amqp_spec.Channel.Flow(channel_number=ch_open2.channel_number)
                flow_ok = yield self.write(flow)

                ex_declare = amqp_spec.Exchange.Declare('message', channel_number=ch_open2.channel_number)
                declare_ok = yield self.write(ex_declare)

                declare_q = amqp_spec.Queue.Declare(queue_name='text', channel_number=ch_open2.channel_number)
                declare_q_ok = yield self.write(declare_q)

                bind = amqp_spec.Queue.Bind(queue_name='text', exchange_name='message', routing_key='text.#', channel_number=ch_open2.channel_number)
                bind_ok = yield self.write(bind)
                for i in range(200):
                    content = 'qrqwrq' + str(i)
                    r = [
                        amqp_spec.Basic.Publish(exchange_name='message', routing_key='text.tratata', channel_number=channel_number),
                        amqp_spec.Header(class_id=amqp_spec.Basic.Publish.class_id, body_size=len(content), header_properties={'content-type': 'application/json'}, channel_number=channel_number),
                        amqp_spec.Content(content=content, channel_number=channel_number)
                    ]
                    publish_methods = r
                    for i in publish_methods:
                        response = yield self.write(i)
                        assert response is None
                yield self.write(amqp_spec.Channel.Close(channel_number=channel_number))
                yield self.write(amqp_spec.Connection.Close())
                yield self.stop()

            def on_close(self):
                pass

        def start_aplication():
            io_loop = ioloop.IOLoop()
            TornadoAplication(ioloop=io_loop)
            io_loop.start()

        start_aplication()

import os.path
import sys

sys.path = [os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib')] + sys.path

from amqpsfw.client import application
from amqpsfw import amqp_spec, ioloop
from amqpsfw.client.configuration import Configuration

class TestApplicationConsumer:
    def test_application_consumer(self):
        class ConsumerAplication(application.Application):
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
                for t in range(100):
                    content = "qwe" + str(t)
                    response = yield self.write(amqp_spec.Basic.Publish(exchange_name='message', routing_key='text.tratata', channel_number=channel_number))
                    assert response is None
                    response = yield self.write(amqp_spec.Header(class_id=amqp_spec.Basic.Publish.class_id, body_size=len(content), header_properties={'content-type': 'application/json'}, channel_number=channel_number))
                    assert response is None
                    response = yield self.write(amqp_spec.Content(content=content, channel_number=channel_number))
                    assert response is None

                consume = amqp_spec.Basic.Consume(queue_name='text', consumer_tag='first_consumer', channel_number=channel_number)
                consume_ok = yield self.write(consume)
                assert type(consume_ok) is amqp_spec.Basic.ConsumeOk
                for i in range(90):
                    delivery = yield
                    # import pdb;pdb.set_trace()
                    assert type(delivery) == amqp_spec.Basic.Deliver
                    header = yield
                    assert type(header) == amqp_spec.Header
                    content = yield
                    assert type(content) == amqp_spec.Content
                    self.write(amqp_spec.Basic.Ack(delivery_tag=delivery.delivery_tag, channel_number=delivery.channel_number))
                yield self.stop()

        def start_aplication():
            io_loop = ioloop.IOLoop()
            app = ConsumerAplication(io_loop)
            io_loop.add_handler(app.socket.fileno(), app.handler, io_loop.READ)
            io_loop.start()

        start_aplication()


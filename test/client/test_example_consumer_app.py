import os.path
import sys

sys.path = [os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib')] + sys.path

from amqpsfw import amqp_spec, ioloop
from amqpsfw.client.client import Client
from amqpsfw.client.configuration import ClientConfiguration


class TestClientConsumer:
    def test_client_consumer(self):
        class ConsumerAplication(Client):
            def processor(self):
                channel_number = 1
                start = yield from super().processor()
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
                result = []
                # TODO Basic.Ack don't required response so if bufer in empty we get NOne if buffer full of vrame we get somethink - fix it
                # TODO if we have intensive input then output is stopped because ioloot get many read events
                for i in range(90):
                    res = yield
                    # log.error('TTTTTTTTTTTTTTTTTTTTTTTTTT: ' + str(res))
                    if res:
                        result.append(res)
                        if type(res) == amqp_spec.Content:
                            # log.error('SSSSSSSSSSSSSSSSSSSSSSSSS: ' + str(result))
                            assert result[0].delivery_tag == (i+1)/3
                            yield self.write(amqp_spec.Basic.Ack(delivery_tag=result[0].delivery_tag, channel_number=result[0].channel_number))
                            assert type(result[0]) == amqp_spec.Basic.Deliver
                            assert type(result[1]) == amqp_spec.Header
                            assert type(result[2]) == amqp_spec.Content
                            result = []
                yield self.stop()

        def start_aplication():
            io_loop = ioloop.IOLoop()
            app = ConsumerAplication(io_loop)
            app.config = ClientConfiguration()
            app.start()
            io_loop.start()

        start_aplication()

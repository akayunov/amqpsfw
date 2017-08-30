import os.path
import sys

sys.path = [os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib')] + sys.path

from amqpsfw.client import application
from amqpsfw import amqp_spec, ioloop


class TestApplicationConsumer:
    def test_application_consumer(self):
        class ConsumerAplication(application.Application):
            def processor(self):
                channel_number = 1
                yield from super().processor()
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


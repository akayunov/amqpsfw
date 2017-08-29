import os.path
import sys

sys.path = [os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib')] + sys.path

from amqpsfw.client import application
from amqpsfw import amqp_spec

from tornado import ioloop


class TestApplicationPublishTornado:
    def test_application_publish_tornado(self):
        class TornadoAplication(application.Application):
            def processor(self):
                channel_number = 1
                yield from super().processor()
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
            app = TornadoAplication(ioloop=io_loop)
            io_loop.add_handler(app.socket.fileno(), app.handler, io_loop.READ)
            io_loop.start()

        start_aplication()

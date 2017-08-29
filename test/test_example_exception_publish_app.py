import os.path
import sys

sys.path = [os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib')] + sys.path

from amqpsfw.client import application
from amqpsfw import amqp_spec, ioloop


class TestApplicationExceptionPublish:
    def test_application_exception_publish(self):
        class PublishAplication(application.Application):
            def processor(self):
                channel_number = 1
                yield from super().processor()
                content = 'qwe0'
                response = yield self.write(amqp_spec.Basic.Publish(exchange_name='message', routing_key='text.tratata', channel_number=channel_number))
                assert response is None
                response = yield self.write(amqp_spec.Header(class_id=amqp_spec.Basic.Publish.class_id, body_size=len(content), header_properties={'content-type': 'application/json'}, channel_number=channel_number))
                assert response is None
                response = yield self.write(amqp_spec.Content(content=content, channel_number=channel_number))
                assert response is None

                # exception part
                con = amqp_spec.Content(content=content, channel_number=channel_number)
                con.encoded = bytes([1,2,3])
                response = yield self.write(con)
                for x in range(100):
                    content = 'qwe' + str(x)
                    response = yield self.write(amqp_spec.Basic.Publish(exchange_name='message', routing_key='text.tratata', channel_number=channel_number))
                    if response:
                        # import pdb;pdb.set_trace()
                        assert type(response) == amqp_spec.Connection.Close
                        break
                    response = yield self.write(amqp_spec.Header(class_id=amqp_spec.Basic.Publish.class_id, body_size=len(content), header_properties={'content-type': 'application/json'},channel_number=channel_number))
                    if response:
                        assert type(response) == amqp_spec.Connection.Close
                        # import pdb;
                        # pdb.set_trace()
                        break
                    response = yield self.write(amqp_spec.Content(content=content, channel_number=channel_number))
                    if response:
                        assert type(response) == amqp_spec.Connection.Close
                        # import pdb;
                        # pdb.set_trace()
                        break
                # assert type(response) is amqp_spec.Connection.Close
                # import pdb;pdb.set_trace()
                # response = yield self.sleep(10)
                # import pdb;pdb.set_trace()s
                # assert type(response) is amqp_spec.Connection.Close

                # response = yield self.write(amqp_spec.Channel.Close(channel_number=channel_number))
                # response = yield self.write(amqp_spec.Connection.Close())
                yield self.stop()


        def start_aplication():
            io_loop = ioloop.IOLoop()
            app = PublishAplication(io_loop)
            io_loop.add_handler(app.socket.fileno(), app.handler, io_loop.READ)
            io_loop.start()

        start_aplication()

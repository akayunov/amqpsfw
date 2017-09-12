import os.path
import sys

sys.path = [os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib')] + sys.path

from amqpsfw import amqp_spec, ioloop
from amqpsfw.client.client import Client


class TestClientPublishException:
    def test_client_publish_exception(self):
        class PublishAplication(Client):

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
                content = 'qwe0'
                response = yield self.write(amqp_spec.Basic.Publish(exchange_name='message', routing_key='text.tratata', channel_number=channel_number))
                assert response is None
                response = yield self.write(amqp_spec.Header(class_id=amqp_spec.Basic.Publish.class_id, body_size=len(content), header_properties={'content-type': 'application/json'}, channel_number=channel_number))
                assert response is None
                response = yield self.write(amqp_spec.Content(content=content, channel_number=channel_number))
                assert response is None

                # exception part
                con = amqp_spec.Content(content=content, channel_number=channel_number)
                con.payload = b''
                response = yield self.write(con)
                for x in range(100):
                    content = 'qwe' + str(x)
                    response = yield self.write(amqp_spec.Basic.Publish(exchange_name='message', routing_key='text.tratata', channel_number=channel_number))
                    if response:
                        # import pdb;pdb.set_trace()
                        assert type(response) == amqp_spec.Connection.Close
                        import pdb;
                        pdb.set_trace()
                        yield self.stop()
                    response = yield self.write(
                        amqp_spec.Header(
                            class_id=amqp_spec.Basic.Publish.class_id, body_size=len(content), header_properties={'content-type': 'application/json'},
                            channel_number=channel_number))
                    if response:
                        assert type(response) == amqp_spec.Connection.Close
                        # import pdb;
                        # pdb.set_trace()
                        import pdb;pdb.set_trace()
                        yield self.stop()
                    response = yield self.write(amqp_spec.Content(content=content, channel_number=channel_number))
                    if response:
                        assert type(response) == amqp_spec.Connection.Close
                        # import pdb;
                        # pdb.set_trace()
                        import pdb;
                        pdb.set_trace()
                        yield self.stop()

                # assert type(response) is amqp_spec.Connection.Close
                # import pdb;pdb.set_trace()
                # response = yield self.sleep(10)
                # import pdb;pdb.set_trace()s
                # assert type(response) is amqp_spec.Connection.Close

                # response = yield self.write(amqp_spec.Channel.Close(channel_number=channel_number))
                # response = yield self.write(amqp_spec.Connection.Close())

        def start_aplication():
            io_loop = ioloop.IOLoop()
            app = PublishAplication(io_loop)
            app.start()
            io_loop.start()

        start_aplication()

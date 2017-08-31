import os.path
import sys
import pytest

sys.path = [os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib')] + sys.path

from amqpsfw.client import application
from amqpsfw import amqp_spec, ioloop
from amqpsfw.client.configuration import Configuration

class TestApplicationExceptionPublish:
    def test_application_exception_publish(self):
        class PublishAplication(application.Application):
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
            PublishAplication(io_loop)
            io_loop.start()

        start_aplication()

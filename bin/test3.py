import os.path
import sys

sys.path = [os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib')] + sys.path

import logging
from amqpsfw import amqp_spec, ioloop
from amqpsfw.server.server import Server, ServerClient
from amqpsfw.client.client import Client
from amqpsfw.logger import init_logger

from amqpsfw.client.configuration import Configuration as CConf
from amqpsfw.server.configuration import Configuration as SConf
class ClientPublishAplication(Client):
    method_mapper = {}

    CConf.port = 5555

    def processor(self):
        channel_number = 1
        start = yield from super().processor()
        start_ok = amqp_spec.Connection.StartOk({'host': CConf.host}, CConf.sals_mechanism, credential=[CConf.credential.user, CConf.credential.password])
        tune = yield self.write(start_ok)

        tune_ok = amqp_spec.Connection.TuneOk(heartbeat_interval=CConf.heartbeat_interval)
        # yield self.write(tune_ok)  # it works too!!!! and frame must be send to server
        self.write(tune_ok)  # it works too!!!! and frame will be send to server on next yield

        c_open = amqp_spec.Connection.Open(virtual_host=CConf.virtual_host)
        openok = yield self.write(c_open)

        # channel_obj = amqp_spec.Channel()
        # ch_open = channel_obj.Open(channel_number=1)
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
            response = yield self.write(amqp_spec.Header(class_id=amqp_spec.Basic.Publish.class_id, body_size=len(content), header_properties={'content-type': 'application/json'},
                                                         channel_number=channel_number))
            assert response is None
            response = yield self.write(amqp_spec.Content(content=content, channel_number=channel_number))
            assert response is None
        response = yield self.write(amqp_spec.Channel.Close(channel_number=channel_number))
        assert type(response) is amqp_spec.Channel.CloseOk
        response = yield self.write(amqp_spec.Connection.Close())
        assert type(response) is amqp_spec.Connection.CloseOk
        yield self.stop()


class ServerAplication(ServerClient):
    method_mapper = {}

    SConf.port = 5555

    def processor(self):
        channel_number = 1
        yield from super().processor()
        # import pdb;pdb.set_trace()
        start = amqp_spec.Connection.Start(0, 9, {'host': SConf.host}, SConf.sals_mechanism)
        start_ok = yield self.write(start)

        tune = amqp_spec.Connection.Tune(heartbeat_interval=SConf.heartbeat_interval)
        # yield self.write(tune_ok)  # it works too!!!! and frame must be send to server
        tune_ok = yield self.write(tune)  # it works too!!!! and frame will be send to server on next yield
        conn_open = yield
        open_ok = amqp_spec.Connection.OpenOk()
        ch_open1 = yield self.write(open_ok)

        ch_open_ok = amqp_spec.Channel.OpenOk()

        flow = yield self.write(ch_open_ok)
        exc_declare = yield self.write(amqp_spec.Channel.FlowOk(channel_number=ch_open1.channel_number))

        queue_declare = yield self.write(amqp_spec.Exchange.DeclareOk())

        queue_bind = yield self.write(amqp_spec.Queue.DeclareOk(queue_declare.queue_name))
        publish = yield self.write(amqp_spec.Queue.BindOk())

        yield self.sleep(3)
        for t in range(100):
            content = "qwe" + str(t)
            response = yield
            print(response)


io_loop = ioloop.IOLoop()
Server(io_loop, ServerAplication)
ClientPublishAplication(io_loop)
io_loop.start()
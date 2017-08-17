#!/home/akayunov/sfw/venv/bin/python3
import sys
sys.path = ['/home/akayunov/sfw/sfw/lib'] + sys.path

import application
import amqp_spec

from tornado import ioloop


class TornadoAplication(application.Application):
    def processor(self):
        channel_number = 1
        # TODO devide it more granular
        # protocol_header = amqp_spec.ProtocolHeader('A', 'M', 'Q', 'P', 0, 0, 9, 1)
        # start = yield self.write(protocol_header)
        start = yield

        channel_number = 1
        start_ok = amqp_spec.Connection.StartOk({'host': ['S', 'localhost']}, 'PLAIN', credential=['root', 'privetserver'])
        tune = yield self.write(start_ok)

        tune_ok = amqp_spec.Connection.TuneOk(heartbeat_interval=1)
        # yield self.write(tune_ok)  # it works too!!!! and frame must be send to server
        self.write(tune_ok)  # it works too!!!! and frame will be send to server on next yield

        c_open = amqp_spec.Connection.Open(virtual_host='/')
        openok = yield self.write(c_open)

        ch_open = amqp_spec.Channel.Open(channel_number=channel_number)
        ch_open_ok = yield self.write(ch_open)

        flow = amqp_spec.Channel.Flow(channel_number=channel_number)
        flow_ok = yield self.write(flow)

        ex_declare = amqp_spec.Exchange.Declare('message', channel_number=channel_number)
        declare_ok = yield self.write(ex_declare)

        declare_q = amqp_spec.Queue.Declare(queue_name='text', channel_number=channel_number)
        declare_q_ok = yield self.write(declare_q)

        bind = amqp_spec.Queue.Bind(queue_name='text', exchange_name='message', routing_key='text.#', channel_number=channel_number)
        bind_ok = yield self.write(bind)

        for i in range(200):
        # k = 1
        # while 1:
        #     k += 1
        #     content = 'qrqwrq' + str(k)
            content = 'qrqwrq' + str(i)
            r = [
                amqp_spec.Basic.Publish(exchange_name='message', routing_key='text.tratata', channel_number=channel_number),
                amqp_spec.Header(class_id=amqp_spec.Basic.Publish.class_id, body_size=len(content), header_properties={'content-type': 'application/json'}, channel_number=channel_number),
                amqp_spec.Content(content=content, channel_number=channel_number)
            ]
            publish_methods = r
            yield self.sleep(1)
            #yield gen.sleep(1)
            for i in publish_methods:
                yield self.write(i)
        yield self.write(amqp_spec.Channel.Close(channel_number=channel_number))
        yield self.write(amqp_spec.Connection.Close())
        exit(0)


def start_aplication():
    io_loop = ioloop.IOLoop()
    # TODO move application on coroutines but it's not mandatory
    app = TornadoAplication(ioloop=io_loop)
    c_socket = app.start()
    io_loop.add_handler(c_socket.fileno(), app.handler, io_loop.READ)
    io_loop.start()

start_aplication()

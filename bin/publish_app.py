#!/home/akayunov/sfw/vienv/bin/python3
import sys
sys.path = ['/home/akayunov/sfw/sfw/lib'] + sys.path

import application
import ioloop
import amqp_spec


class PublishAplication(application.Application):
    def processor(self):
        protocol_header = amqp_spec.ProtocolHeader('A', 'M', 'Q', 'P', 0, 0, 9, 1)
        start = yield self.write(protocol_header)

        channel_number = 1
        start_ok = amqp_spec.Connection.StartOk({'host': ['S', 'localhost']}, 'PLAIN', credential=['root', 'privetserver'])
        tune = yield self.write(start_ok)

        tune_ok = amqp_spec.Connection.TuneOk(heartbeat_interval=100)
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
            content = 'qrqwrq' + str(i)
            r = [
                amqp_spec.Basic.Publish(exchange_name='message', routing_key='text.tratata', channel_number=channel_number),
                amqp_spec.Header(class_id=amqp_spec.Basic.Publish.class_id, body_size=len(content), properties={'content-type': 'application/json'}, channel_number=channel_number),
                amqp_spec.Content(content=content, channel_number=channel_number)
            ]
            publish_methods = r
            for i in publish_methods:
                yield self.write(i)
                #yield self.sleep(1)


def start_aplication():
    app = PublishAplication()
    c_socket = app.start()
    io_loop = ioloop.IOLoop()
    io_loop.add_handler(c_socket.fileno(), app, io_loop.read)
    io_loop.start()

start_aplication()

#!/home/akayunov/sfw/vienv/bin/python3
import sys
sys.path = ['/home/akayunov/sfw/sfw/lib'] + sys.path

import application
import ioloop
import amqp_spec


class ConsumerAplication(application.Application):
    def processor(self):
        channel_number = 1
        yield from self.connect()
        consume = amqp_spec.Basic.Consume(queue_name='text', consumer_tag='first_consumer', channel_number=channel_number)
        consume_ok = yield self.write(consume)

        while 1:
            delivery = yield
            # header = yield
            # content = yield
            #import pdb;pdb.set_trace()
            print('XXAXXAX: ', delivery)
            if type(delivery) == amqp_spec.Basic.Deliver:
                self.write(amqp_spec.Basic.Ack(delivery_tag=delivery.delivery_tag, channel_number=delivery.channel_number))
                yield


def start_aplication():
    app = ConsumerAplication()
    c_socket = app.start()
    io_loop = ioloop.IOLoop()
    io_loop.add_handler(c_socket.fileno(), app, io_loop.read)
    io_loop.start()

start_aplication()

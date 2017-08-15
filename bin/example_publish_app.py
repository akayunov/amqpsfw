#!/home/akayunov/sfw/vienv/bin/python3
import sys
sys.path = ['/home/akayunov/sfw/sfw/lib'] + sys.path

import application
import ioloop
import amqp_spec


class PublishAplication(application.Application):
    def processor(self):
        channel_number = 1
        yield from self.connect()

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
        yield self.write(amqp_spec.Channel.Close(channel_number=channel_number))
        yield self.write(amqp_spec.Connection.Close())
        exit(0)


def start_aplication():
    app = PublishAplication()
    c_socket = app.start()
    io_loop = ioloop.IOLoop()
    io_loop.add_handler(c_socket.fileno(), app, io_loop.read)
    io_loop.start()

start_aplication()

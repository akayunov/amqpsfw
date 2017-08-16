#!/home/akayunov/sfw/venv/bin/python3
import sys
sys.path = ['/home/akayunov/sfw/sfw/lib'] + sys.path

import application
import ioloop
import amqp_spec


class PublishAplication(application.Application):
    def processor(self):
        channel_number = 1
        yield from super().processor()

        #for i in range(200):
        k = 1
        while 1:
            k += 1
            content = 'qrqwrq' + str(k)
            r = [
                amqp_spec.Basic.Publish(exchange_name='message', routing_key='text.tratata', channel_number=channel_number),
                amqp_spec.Header(class_id=amqp_spec.Basic.Publish.class_id, body_size=len(content), properties={'content-type': 'application/json'}, channel_number=channel_number),
                amqp_spec.Content(content=content, channel_number=channel_number)
            ]
            publish_methods = r
            yield self.sleep(1)
            for i in publish_methods:
                yield self.write(i)
        yield self.write(amqp_spec.Channel.Close(channel_number=channel_number))
        yield self.write(amqp_spec.Connection.Close())
        exit(0)


def start_aplication():
    io_loop = ioloop.IOLoop()
    app = PublishAplication(io_loop)
    c_socket = app.start()
    io_loop.add_handler(c_socket.fileno(), app.handler, io_loop.READ)
    #########################
    # TODO remove it
    app.processor = app.processor()
    app.processor.send(None)
    #########################
    io_loop.start()

start_aplication()

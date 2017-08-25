import os.path
import sys

sys.path = [os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib')] + sys.path

from amqpsfw.client import application
from amqpsfw import amqp_spec, ioloop


class PublishAplication(application.Application):
    def processor(self):
        channel_number = 1
        yield from super().processor()
        for t in range(100):
            content = "qwe" +  str(t)
            response = yield self.sleep(1)
            if type(response) is amqp_spec.Connection.Close:
                print('Break')
                exit(0)
            response = yield self.write(amqp_spec.Basic.Publish(exchange_name='message', routing_key='text.tratata', channel_number=channel_number))
            if type(response) is amqp_spec.Connection.Close:
                print('Break')
                exit(0)
            response = yield self.write(amqp_spec.Header(class_id=amqp_spec.Basic.Publish.class_id, body_size=len(content), header_properties={'content-type': 'application/json'}, channel_number=channel_number))
            if type(response) is amqp_spec.Connection.Close:
                print('Break')
                exit(0)
            if t == 3:
                con = amqp_spec.Content(content=content, channel_number=channel_number)
                con.encoded = bytes([1,2,3])
                response = yield self.write(con)
            else:
                response = yield self.write(amqp_spec.Content(content=content, channel_number=channel_number))
            if type(response) is amqp_spec.Connection.Close:
                print('Break')
                exit(0)

        response = yield self.write(amqp_spec.Channel.Close(channel_number=channel_number))
        response = yield self.write(amqp_spec.Connection.Close())
        exit(0)


def start_aplication():
    io_loop = ioloop.IOLoop()
    app = PublishAplication(io_loop)
    c_socket = app.start()
    io_loop.add_handler(c_socket.fileno(), app.handler, io_loop.READ)
    io_loop.start()

start_aplication()

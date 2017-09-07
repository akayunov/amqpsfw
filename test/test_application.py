import os
import sys
import socket
sys.path = [os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib')] + sys.path

from amqpsfw import amqp_spec
from amqpsfw.application import Application
from amqpsfw.ioloop import IOLoop


class AppTest(Application):
    def start(self):
        self.result = []
        self.app_gen = self.processor()
        (ssock, csock) = socket.socketpair()
        self.socket = ssock
        self.csock = csock
        self.app_gen.send(None)

    def processor(self):
        while 1:
            frame = yield
            self.result.append(frame)


class TestAmqpSpec:
    def test_handle_read_one_by_one(self):
        app = AppTest(IOLoop())
        app.start()
        expected = []
        for i in range(10):
            data = amqp_spec.Basic.Publish('exc_name', 'routing')
            app.csock.send(data.encoded)
            app.handle_read()
            expected.append(data)
            data = amqp_spec.Content('qwe' + str(i))
            app.csock.send(data.encoded)
            app.handle_read()
            expected.append(data)
        assert expected == app.result

    def test_handle_read_bunch(self):
        app = AppTest(IOLoop())
        app.start()
        expected = []
        for i in range(10):
            data = amqp_spec.Basic.Publish('exc_name', 'routing')
            app.csock.send(data.encoded)
            expected.append(data)
            data = amqp_spec.Content('qwe' + str(i))
            app.csock.send(data.encoded)
            expected.append(data)
        for _ in range(20):
            app.handle_read()
        assert expected == app.result

    def test_handle_read_not_full_frame(self):
        app = AppTest(IOLoop())
        app.start()
        expected = []
        for i in range(5):
            data = amqp_spec.Basic.Publish('exc_name', 'routing')
            app.csock.send(data.encoded)
            expected.append(data)
            data = amqp_spec.Content('qwe' + str(i))
            app.csock.send(data.encoded)
            expected.append(data)
        data = amqp_spec.Basic.Publish('exc_name', 'routing')
        app.csock.send(data.encoded[0:3])
        expected.append(data)
        for _ in range(10):
            app.handle_read()
        app.csock.send(data.encoded[3:])
        for i in range(5):
            data = amqp_spec.Basic.Publish('exc_name', 'routing')
            app.csock.send(data.encoded)
            expected.append(data)
            data = amqp_spec.Content('qwe' + str(i))
            app.csock.send(data.encoded)
            expected.append(data)

        for _ in range(11):
            app.handle_read()
        assert expected == app.result

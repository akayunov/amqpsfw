import os.path
import sys

sys.path = [os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib')] + sys.path

import logging
from amqpsfw import amqp_spec, ioloop
from amqpsfw.server.server import Server
from amqpsfw.logger import init_logger

log = logging.getLogger(__name__)
init_logger()


class TestServer:
    def test_server_basic(self):
        io_loop = ioloop.IOLoop()
        Server(io_loop)
        io_loop.start()

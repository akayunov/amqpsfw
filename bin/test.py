import os.path
import sys

sys.path = [os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib')] + sys.path

from amqpsfw import amqp_spec
from amqpsfw import ioloop
from amqpsfw.server.server import Server

io_loop = ioloop.IOLoop()
Server(io_loop)
io_loop.start()
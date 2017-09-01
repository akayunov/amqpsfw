import os.path
import sys

sys.path = [os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib')] + sys.path

from amqpsfw.server.server_application import Server
from amqpsfw import ioloop


def main():
    io_loop = ioloop.IOLoop()
    Server(io_loop)
    io_loop.start()

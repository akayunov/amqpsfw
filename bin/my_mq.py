#!/home/akayunov/sfw/vienv/bin/python3
import sys
sys.path = ['/home/akayunov/sfw/sfw/lib'] + sys.path

import application
import ioloop


def start_aplication():
    app = application.Application()
    c_socket = app.start()
    io_loop = ioloop.IOLoop()
    io_loop.add_handler(c_socket.fileno(), app, io_loop.read)
    io_loop.start()

start_aplication()

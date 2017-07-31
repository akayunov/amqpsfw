import time
from functools import partial
import tornado
from tornado.websocket import websocket_connect
from tornado.gen import coroutine


# TODO do some block operation and do it coroutine
def on_connect(future):
    print('Connection is made')


# corutine style
@coroutine
def handle_read(conn):
    for i in range(10):
        msg = yield conn.read_message()
        print(msg)


@coroutine
def on_msg(msg):
    print('Clinet msg: ' + str(msg))
    if msg is None:
        tornado.ioloop.IOLoop.current().stop()
        # yield
        # print('IOLOop was stoppped')

i =1
@coroutine
def handle_write(conn):
    global i
    i +=1
    msg = 'tratata' + str(i)
    # TODO print is blocking do it thorugth coroutine
    #print(msg)
    yield conn.write_message(msg)


@coroutine
def do_connection():
    wsconn_corutine = yield websocket_connect(
        url='ws://localhost:4433/ws', callback=on_connect, connect_timeout=5,
                      on_message_callback=on_msg, compression_options={},
                      ping_interval=1, ping_timeout=2,
                      max_message_size=1000)
    return wsconn_corutine


def on_ping(self, data):
    print('In on_ping')


def on_pong(self, data):
    print('In on_pong')

@coroutine
def main_coro():
    wsconn_corutine = yield do_connection()
    wsconn_corutine.on_ping = partial(on_ping, wsconn_corutine)
    wsconn_corutine.on_pong = partial(on_pong, wsconn_corutine)
    while 1:
        yield handle_write(wsconn_corutine)
        time.sleep(1)
    #yield handle_read(wsconn_corutine)


io_loop = tornado.ioloop.IOLoop.current()
#io_loop.start()
io_loop.run_sync(main_coro)




# # callback style
# # TODO do some block operation and do it coroutine
# def on_message(msg, *args, **kwargs):
#     print(msg)
#     print(args)
#     print(kwargs)
#
# wsconn_callback = websocket_connect(url='https://localhost:4433/ws', callback=on_connect, connect_timeout=5,
#                       on_message_callback=None, compression_options={},
#                       ping_interval=1, ping_timeout=2,
#                       max_message_size=1000)
#
# wsconn_callback.close()
#

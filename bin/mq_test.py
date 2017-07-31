import time
import json
import pikamy
import pikamy.spec
from pikamy import TConnection

from tornado.gen import coroutine
from tornado.concurrent import Future
from tornado.ioloop import IOLoop


@coroutine
def mq_work_flow():
    connection = TConnection(
        parameters=pikamy.URLParameters('amqp://root:privetserver@localhost:5672/%2F?connection_attempts=3&heartbeat_interval=3600'),
        # on_open_callback=self.on_connection_open,
        # on_open_error_callback=None,
        # on_close_callback=None,
        # stop_ioloop_on_close=False,
        # custom_ioloop=None
    )
    print()
    connection._send_connection_open()
    connection._set_connection_state(connection.CONNECTION_START)
    # if self._is_protocol_header_frame(method_frame):
    #     raise exceptions.UnexpectedFrameError
    # self._check_for_protocol_mismatch(method_frame)

    print(connection._frame_buffer)
    fr = connection._read_frame()
    print(fr)
    connection._set_server_information()
    connection._on_connection_tune(connection._read_frame())
    connection._send_connection_start_ok(*connection._get_credentials(connection._read_frame()))
    time.sleep(100)
    # channel = connection.channel(on_open_callback=lambda x: x)
    # channel.open()
    # channel._set_state(channel.OPEN)
    # channel.exchange_declare(exchange='tratata', callback=lambda x: x)
    # channel.confirm_delivery()
    # message = {'i': 1}
    # properties = pikamy.BasicProperties(app_id='example-publisher',
    #                                   content_type='application/json',
    #                                   headers=message)
    #
    # channel.basic_publish('tratata', 'msg.text',
    #                             json.dumps(message, ensure_ascii=False),
    #                             properties)


def on_finish(f):
    print('finish')
    print(f.result())
    ioloop.stop()


ioloop = IOLoop.current()
ioloop.add_future(mq_work_flow(), on_finish)
ioloop.start()
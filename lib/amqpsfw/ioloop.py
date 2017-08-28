import logging
import time
import select
from functools import partial
IOLOOP = None
# TODO integration with asyncio

log = logging.getLogger(__name__)


class IOLoopException(Exception):
    def __init__(self, code, msg):
        self.code = code
        self.msg = msg

    def __str__(self):
        return str(type(self)) + ': ' + str(self.code) + ' - ' + self.msg


class IOLoop:
    READ = select.EPOLLIN
    WRITE = select.EPOLLOUT

    def __new__(cls, *args, **kwargs):
        global IOLOOP
        if not IOLOOP:
            self = super().__new__(cls)
            IOLOOP = self
        else:
            self = IOLOOP

        return self

    def __init__(self):
        self.fileno = None
        self.app = None
        self.app_processor = None
        self.handler = None
        self.impl = select.epoll()
        # TODO use heapq to sort callbacks by timeout
        self.callbacks = {}

    @staticmethod
    def current():
        return IOLOOP

    def call_later(self, duration, func, *args, **kwargs):
        when = int(time.time()) + duration
        if when in self.callbacks:
            self.callbacks[when].append(partial(func, *args, **kwargs))
        else:
            self.callbacks[when] = [partial(func, *args, **kwargs)]

    def add_handler(self, fileno, handler, io_state):
        self.fileno = fileno
        self.impl.register(fileno, select.EPOLLERR | io_state)
        self.handler = handler

    def update_handler(self, fd, events):
        self.impl.modify(fd, events)

    def unregistered(self):
        self.impl.unregister(self.fileno)

    def run_callbacks(self):
        current_time = time.time()
        callback_times = sorted(self.callbacks.keys())
        for callback_time in callback_times:
            if callback_time - current_time <= 0:
                for c in self.callbacks[callback_time]:
                    c()
                del self.callbacks[callback_time]
            else:
                return callback_time - current_time
        return -1

    def start(self):
        while 1:
            next_timeout_callback = self.run_callbacks()
            events = self.impl.poll(next_timeout_callback)  # TODO signals interupt this call??
            log.debug('POOL: %s %s %s', str(int(time.time())), next_timeout_callback, events)
            for fd, event in events:
                # TODO add more event type checking
                if event & (select.EPOLLIN | select.EPOLLPRI | select.EPOLLRDBAND):
                    log.debug('IN: %s %s %s', str(int(time.time())), events, next_timeout_callback)
                    self.handler(self.fileno, event)
                if event & select.EPOLLOUT:
                    log.debug('OUT: %s %s %s',  str(int(time.time())), events, next_timeout_callback)
                    self.handler(self.fileno, event)
                if event & select.EPOLLHUP:
                    pass
                if event & select.EPOLLERR:
                    pass
                if event & select.EPOLLRDHUP:
                    pass
                # else:
                #     raise IOLoopException('IOLOOP', 'Unknown error socket state')
            if not events:
                self.run_callbacks()

    def stop(self):
        # TODO flush buffers
        pass
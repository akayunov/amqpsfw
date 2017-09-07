import logging
import time
import select
from functools import partial

from amqpsfw.utils import map_event_to_io_state

IOLOOP = None
log = logging.getLogger(__name__)
STOPPED = 'STOPPED'
RUNNING = 'RUNNING'


class IOLoop:
    def __new__(cls, *args, **kwargs):
        global IOLOOP
        if not IOLOOP:
            self = super().__new__(cls)
            IOLOOP = self
        else:
            self = IOLOOP

        return self

    def __init__(self):
        self.handler = {}
        self.impl = select.epoll()
        # TODO use heapq to sort callbacks by timeout
        self.callbacks = {}
        self.status = RUNNING

    @staticmethod
    def current():
        return IOLOOP

    def call_later(self, duration, func, *args, **kwargs):
        when = int(time.time()) + duration
        if when in self.callbacks:
            self.callbacks[when].append(partial(func, *args, **kwargs))
        else:
            self.callbacks[when] = [partial(func, *args, **kwargs)]

    def add_handler(self, fd, handler, events):
        log.debug('%s %s %s', fd, handler, map_event_to_io_state(events))
        self.impl.register(fd, events)
        self.handler[fd] = handler

    def update_handler(self, fd, events):
        log.debug('%s %s', fd, map_event_to_io_state(events))
        self.impl.modify(fd, events)

    def unregistered(self, fd):
        log.debug('%s', fd)
        del self.handler[fd]
        self.impl.unregister(fd)

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
        while self.status == RUNNING:
            next_timeout_callback = self.run_callbacks()
            events = self.impl.poll(next_timeout_callback)  # TODO signals interupt this call??
            for fd, event in events:
                log.debug('POOL: %s %s %s', next_timeout_callback, fd, map_event_to_io_state(event))
                self.handler[fd](fd, event)
            if not events:
                self.run_callbacks()

    def stop(self):
        self.status = STOPPED

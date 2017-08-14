import time
import select

IOLOOP = None
EPOOL = select.epoll()


class IOLoopException(Exception):
    def __init__(self, code, msg):
        self.code = code
        self.msg = msg

    def __str__(self):
        return str(type(self)) + ': ' + str(self.code) + ' - ' + self.msg


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
        self.read = select.EPOLLIN
        self.write = select.EPOLLOUT
        self.timeout_time_expired = None
        self.fileno = None
        self.app = None
        self.app_processor = None

    @staticmethod
    def current():
        return IOLOOP

    def add_handler(self, fileno, app, io_state):
        self.fileno = fileno
        EPOOL.register(fileno, select.EPOLLERR | io_state)
        self.app = app

        # TODO remove it
        self.app_processor = app.processor()
        app.processor = self.app_processor
        self.app_processor.send(None)

    def modify_to_read(self, timeout_in_seconds=None):
        EPOOL.modify(self.fileno, select.EPOLLIN | select.EPOLLERR | select.EPOLLPRI | select.EPOLLRDBAND | select.EPOLLHUP | select.EPOLLRDHUP)
        if timeout_in_seconds:
            self.timeout_time_expired = time.time() + timeout_in_seconds

    def modify_to_write(self, timeout_in_seconds=None):
        EPOOL.modify(self.fileno, select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP | select.EPOLLRDHUP)
        if timeout_in_seconds:
            self.timeout_time_expired = time.time() + timeout_in_seconds

    def unregistered(self):
        EPOOL.unregister(self.fileno)

    def handle_read(self, by_timeout=False):
        self.app.handle_read(by_timeout=by_timeout)

    def handle_write(self):
        self.app.handle_write()

    def start(self):
        while 1:
            if self.timeout_time_expired:
                timeout = (self.timeout_time_expired - int(time.time()))
                if timeout < 0:
                    self.timeout_time_expired = None
                    timeout = -1
                    # timeout happend, sleep timeout passed
                    self.timeout_time_expired = None
                    self.handle_read(by_timeout=True)
            else:
                timeout = -1
            print('POOL ', str(int(time.time())), timeout, self.timeout_time_expired)
            events = EPOOL.poll(timeout)
            for fd, event in events:
                # TODO add more event type checking
                if event & (select.EPOLLIN | select.EPOLLPRI | select.EPOLLRDBAND):
                    # print('IN: ', str(int(time.time())),events, timeout, self.timeout_time_expired)
                    self.handle_read()
                elif event & select.EPOLLOUT:
                    # print('OUT: ' ,  str(int(time.time())), events, timeout, self.timeout_time_expired)
                    self.handle_write()
                elif event & select.EPOLLHUP:
                    pass
                elif event & select.EPOLLERR:
                    pass
                elif event & select.EPOLLRDHUP:
                    pass
                else:
                    raise IOLoopException('IOLOOP', 'Unknown error socket state')
            if not events:
                # timeout happend, sleep timeout passed
                # print('No: ',  str(int(time.time())) , events, timeout, self.timeout_time_expired)
                self.timeout_time_expired = None
                self.handle_read(by_timeout=True)

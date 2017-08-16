import time
import select

IOLOOP = None


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
        self.timeout_time_expired = None
        self.fileno = None
        self.app = None
        self.app_processor = None
        self.handler = None
        self.impl = select.epoll()

    @staticmethod
    def current():
        return IOLOOP

    def add_handler(self, fileno, handler, io_state):
        self.fileno = fileno
        self.impl.register(fileno, select.EPOLLERR | io_state)
        self.handler = handler

    def update_handler(self, fd, events):
        self.impl.modify(fd, events)

    def unregistered(self):
        self.impl.unregister(self.fileno)

    def start(self):
        while 1:
            timeout = -1
            events = self.impl.poll(timeout)
            print('POOL ', str(int(time.time())), timeout, self.timeout_time_expired, events)
            for fd, event in events:
                # TODO add more event type checking
                if event & (select.EPOLLIN | select.EPOLLPRI | select.EPOLLRDBAND):
                    # print('IN: ', str(int(time.time())),events, timeout, self.timeout_time_expired)
                    self.handler(self.fileno, event)
                elif event & select.EPOLLOUT:
                    # print('OUT: ' ,  str(int(time.time())), events, timeout, self.timeout_time_expired)
                    self.handler(self.fileno, event)
                elif event & select.EPOLLHUP:
                    pass
                elif event & select.EPOLLERR:
                    pass
                elif event & select.EPOLLRDHUP:
                    pass
                else:
                    raise IOLoopException('IOLOOP', 'Unknown error socket state')
            if not events:
                # timeout occur, sleep timeout passed
                # print('No: ',  str(int(time.time())) , events, timeout, self.timeout_time_expired)
                self.timeout_time_expired = None
                self.handler(self.fileno, (select.EPOLLIN | select.EPOLLPRI | select.EPOLLRDBAND))

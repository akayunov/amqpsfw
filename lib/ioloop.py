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

    def modify_to_read(self):
        EPOOL.modify(self.fileno, select.EPOLLIN | select.EPOLLERR | select.EPOLLPRI | select.EPOLLRDBAND | select.EPOLLHUP | select.EPOLLRDHUP)

    def modify_to_write(self):
        EPOOL.modify(self.fileno, select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP | select.EPOLLRDHUP)

    def unregistered(self):
        EPOOL.unregister(self.fileno)

    def handle_read(self):
        self.app.handle_read()

    def handle_write(self):
        self.app.handle_write()

    def start(self):
        while 1:
            events = EPOOL.poll()
            for fd, event in events:
                # TODO add more event type checking
                if event & (select.EPOLLIN | select.EPOLLPRI | select.EPOLLRDBAND):
                    self.handle_read()
                elif event & select.EPOLLOUT:
                    self.handle_write()
                elif event & select.EPOLLHUP:
                    pass
                elif event & select.EPOLLERR:
                    pass
                elif event & select.EPOLLRDHUP:
                    pass
                else:
                    raise IOLoopException('IOLOOP', 'Unknown error socket state')

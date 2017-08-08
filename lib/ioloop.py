import select
from exceptions import SfwException
IOLOOP = None
EPOOL = select.epoll()

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
        self.read = 'READ'

    @staticmethod
    def current():
        return IOLOOP

    def add_handler(self, fileno, app, io_state):
        self.app = app
        self.app_processor = app.processor_new()
        self.fileno = fileno
        EPOOL.register(fileno, select.EPOLLERR | select.EPOLLIN)

    def modify_to_read(self):
        EPOOL.modify(self.fileno, select.EPOLLERR | select.EPOLLIN)

    def modify_to_write(self):
        EPOOL.modify(self.fileno, select.EPOLLERR | select.EPOLLOUT)

    def unregistered(self):
        EPOOL.unregister(self.fileno)

    def start(self):
        self.app_processor.send(None)
        while 1:
            events = EPOOL.poll()
            for fd, event in events:
                # TODO we need to devide diferent channales for diferent coroutines
                if event & select.EPOLLIN:
                    self.app.buffer_in += self.app.socket.recv(4096)
                    frame = self.app.parse_buffer()
                    if frame:
                        self.app_processor.send(frame)
                elif event & select.EPOLLOUT:
                    # TODO use more optimize structure for slice
                    writed_bytes = self.app.socket.send(self.app.output_buffer)
                    self.app.output_buffer = self.app.output_buffer[writed_bytes:]
                    if not self.app.output_buffer:
                        self.modify_to_read()
                elif event & select.EPOLLHUP:
                    pass
                else:
                    raise SfwException('IOLOOP', 'Unknown error socket state')
            #import pdb;pdb.set_trace()
            #print(1)

            # self.app.processor()

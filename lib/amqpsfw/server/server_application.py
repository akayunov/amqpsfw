from amqpsfw.application import Application


class Server(Application):
    def start(self):
        pass
    def processor(self):
        protocol_header = yield from super().processor()
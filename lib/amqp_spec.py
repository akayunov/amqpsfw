import struct
from amqp_types import AmqpType, Octet, ShortUint, LongUint


class ProtocolHeader:
    def __init__(self):
        self.encoded = struct.pack('ccccBBBB', b'A', b'M', b'Q', b'P', 0, 0, 9, 1)


class Frame:
    frame_end = Octet(206)

    def __init__(self, channel_number=ShortUint(0), arguments=b''):
        self._encoded = b''
        self.channel_number = channel_number
        self.payload = AmqpType().join(arguments)
        self.encoded = (self.frame_type + self.channel_number + LongUint(len(self.payload)) + self.payload).encoded

    @property
    def encoded(self):
        return self._encoded + self.frame_end.encoded

    @encoded.setter
    def encoded(self, value):
        self._encoded = value

    def __str__(self):
        return str(self.encoded)


class Method(Frame):
    frame_type = Octet(1)

    def __init__(self, channel_number=ShortUint(0), arguments=b''):
        self.channel_number = channel_number
        self.payload = ShortUint(self.class_id) + ShortUint(self.method_id) + AmqpType().join(arguments)
        self.encoded = (self.frame_type + self.channel_number + LongUint(len(self.payload)) + self.payload).encoded


class Header(Frame):
    frame_type = Octet(2)


class Content(Frame):
    frame_type = Octet(3)


class Heartbeat(Frame):
    frame_type = Octet(8)

    def __init__(self):
        super().__init__(channel_number=ShortUint(0))


class Connection:
    class StartOk(Method):
        class_id = 10
        method_id = 11

    class TuneOk(Method):
        class_id = 10
        method_id = 31

    class Open(Method):
        class_id = 10
        method_id = 40


class Channel:
    class Open(Method):
        class_id = 20
        method_id = 10

    class Flow(Method):
        class_id = 20
        method_id = 20


class Exchange:
    class Declare(Method):
        class_id = 40
        method_id = 10


class Queue:
    class Declare(Method):
        class_id = 50
        method_id = 10

    class Bind(Method):
        class_id = 50
        method_id = 20


class Basic:
    class Publish(Method):
        class_id = 60
        method_id = 40

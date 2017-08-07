import struct
from amqp_types import AmqpType, Octet, ShortUint, LongUint, FieldTable, ShortString, LongString, Char
from exceptions import SfwException

# TODO do all classes byte like object to remove encode from socket.send


class ProtocolHeader:
    type_structure = [Char] * 4 + [Octet] * 4

    def __init__(self, version):
        self.encoded = (AmqpType() + struct.pack('cccc', b'A', b'M', b'Q', b'P') + AmqpType().join(version)).encoded

    @classmethod
    def decode_frame(cls, frame_bytes):
        result = []
        for i in cls.type_structure:
            # TODO use memoryview
            frame_part, frame_bytes = frame_bytes[: i.get_len()], frame_bytes[i.get_len():]
            result.append(i.decode(frame_part))
        return ProtocolHeader(result[4:])


class Frame:
    frame_end = Octet(206)

    type_structure = [Octet, ShortUint, LongUint]
    def __init__(self, channel_number=ShortUint(0), arguments=b''):
        self._encoded = b''
        self.channel_number = channel_number
        self.payload = AmqpType().join(arguments)
        self.encoded = (self.frame_type + self.channel_number + LongUint(len(self.payload)) + self.payload).encoded

    @classmethod
    def decode_frame(cls, frame_bytes):
        result = []
        # TODO use memoryview

        frame_type, frame_channel, frame_size, frame_payload, frame_end = Octet.decode(bytes([frame_bytes[0]])), ShortUint.decode(frame_bytes[1:3]), LongUint.decode(frame_bytes[3:7]), frame_bytes[7: len(frame_bytes)-1], frame_bytes[-1]
        if frame_end != frame_end:
            raise SfwException('Internal', 'Wrong frame end')
        if frame_size != len(frame_payload):
            raise SfwException('Internal', 'Wrong frame size')
        r = cls.frame_type_map[frame_type.decoded_value()].decode_method(frame_channel, frame_size, frame_payload)
        print(r)
        return r

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

    # TODO think about merge with Frame and remove custom init
    def __init__(self, channel_number=ShortUint(0), arguments=b''):
        self._encoded = b''
        self.channel_number = channel_number
        self.payload = ShortUint(self.class_id) + ShortUint(self.method_id) + AmqpType().join(arguments)
        self.encoded = (self.frame_type + self.channel_number + LongUint(len(self.payload)) + self.payload).encoded
        self.type_structure = [type(self.frame_type), type(self.channel_number), LongUint, ShortUint, ShortUint] + [type(i) for i in arguments] + [Octet]

    @classmethod
    def decode_method(cls, frame_channel, frame_size, method_bytes):
        class_id, method_id, method_payload = ShortUint.decode(method_bytes[0:2]), ShortUint.decode(method_bytes[2:4]),method_bytes[4: len(method_bytes) - 1]
        return cls.method_class_id_map[class_id][method_id].decode_payload(method_payload)

class Header(Frame):
    frame_type = Octet(2)


class Content(Frame):
    frame_type = Octet(3)


class Heartbeat(Frame):
    frame_type = Octet(8)


class Connection:
    class StartOk(Method):
        type_structure = [FieldTable, ShortString, LongString, ShortString]
        class_id = 10
        method_id = 11

        @classmethod
        def decode_payload(cls, payload_bytes):
            result = []
            for i in cls.type_structure:
                # TODO use memoryview
                i_len = i.get_len(payload_bytes)
                payload_part, payload_bytes = payload_bytes[: i_len], payload_bytes[i_len:]
                result.append(i.decode(payload_part))
            return cls(arguments=result)

    class Start(Method):
        type_structure = [Octet, Octet, FieldTable, LongString, LongString]
        class_id = 10
        method_id = 10

        @classmethod
        def decode_payload(cls, payload_bytes):
            result = []
            for i in cls.type_structure:
                # TODO use memoryview
                i_len = i.get_len(payload_bytes)
                payload_part, payload_bytes = payload_bytes[: i_len], payload_bytes[i_len:]
                if type(payload_part) == int:
                    payload_part = bytes([payload_part])
                result.append(i.decode(payload_part))
            return cls(arguments=result)

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


Frame.frame_type_map = {
    # TODO construct it dynamic in runtime
        1: Method,
        2: Header,
        3: Content,
        8: Heartbeat,
    }


Method.method_class_id_map = {
        # TODO construct it dynamic in runtime
        10: {
            10: Connection.Start,
            11: Connection.StartOk,
            31: Connection.TuneOk,
            40: Connection.Open,
        },
        20: {
            10: Channel.Open,
            20: Channel.Flow
        },
        40: {
            10: Exchange.Declare
        },
        50: {
            10: Queue.Declare,
            20: Queue.Bind
        },
        60: {
            40: Basic.Publish
        },
    }

def decode_frame(data_in):
    # we read in buffer whole frame that is why we can parse it easely
    if len(data_in) < 8:
        # data too small len(frame with empty payload) = 8 bytes
        return
    if data_in.startswith(b'AMQP'):
        # protocol header
        # TODO parse version
        if data_in == ProtocolHeader().encoded:
            return ProtocolHeader()
    elif data_in.startswith(0x1):
        # method
        pass
    elif data_in.startswith(0x2):
        # header
        pass
    elif data_in.startswith(0x3):
        # body
        pass
    elif data_in.startswith(0x8):
        # heartbeat
        pass

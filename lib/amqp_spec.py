import struct
from amqp_types import AmqpType, Octet, ShortUint, LongUint, FieldTable, ShortString, LongString, Char, Path, LongLongUint, ExchangeName
from exceptions import SfwException

# TODO do all classes byte like object to remove encode from socket.send


class ProtocolHeader:
    type_structure = [Char] * 4 + [Octet] * 4

    def __init__(self, version):
        self.encoded = (AmqpType() + struct.pack('cccc', b'A', b'M', b'Q', b'P') + AmqpType().join(version)).encoded

    # @classmethod
    # def decode_frame(cls, frame_bytes):
    #     result = []
    #     for i in cls.type_structure:
    #         # TODO use memoryview
    #         frame_part, frame_bytes = frame_bytes[: i.get_len()], frame_bytes[i.get_len():]
    #         frame_part, frame_bytes = i.decode(frame_bytes)
    #         result.append(frame_part)
    #     return ProtocolHeader(result[4:])

    @classmethod
    def decode_frame(cls, payload_bytes):
        # TODO fix it
        result = []
        for i in cls.type_structure:
            # TODO use memoryview
            payload_part, payload_bytes = i.decode(payload_bytes)
            result.append(payload_part)
        return cls(arguments=result)

    def __str__(self):
        return str(type(self)) + ' ' + str(self.encoded)


class Frame:
    frame_end = Octet(206)


    type_structure = [Octet, ShortUint, LongUint]

    def __init__(self, channel_number=ShortUint(0), arguments=b''):
        self._encoded = b''
        self.payload = AmqpType()
        self.channel_number = channel_number
        self.set_payload(arguments)
        self.encoded = (self.frame_type + self.channel_number + LongUint(len(self.payload)) + self.payload).encoded

    def set_payload(self, arguments):
        self.payload = AmqpType().join(arguments)

    @classmethod
    def decode_frame(cls, frame_bytes):
        # TODO use memoryview
        #import pdb;pdb.set_trace()
        (frame_type, _), (frame_channel, _), (frame_size, _),  frame_payload, frame_end = Octet.decode(bytes([frame_bytes[0]])), ShortUint.decode(frame_bytes[1:3]), LongUint.decode(frame_bytes[3:7]), frame_bytes[7: len(frame_bytes)-1], frame_bytes[-1]
        if frame_end != frame_end:
            raise SfwException('Internal', 'Wrong frame end')
        #if frame_size != len(frame_payload):
        #    raise SfwException('Internal', 'Wrong frame size')
        # TODO parent should not known about child do it on module level with function
        r = cls.frame_type_map[frame_type.decoded_value()].decode_method(frame_channel, frame_size, frame_payload)
        return r

    @property
    def encoded(self):
        return self._encoded + self.frame_end.encoded

    @encoded.setter
    def encoded(self, value):
        self._encoded = value

    def __str__(self):
        return str(type(self)) + ' ' + str(self.encoded)


class Method(Frame):
    frame_type = Octet(1)

    def set_payload(self, arguments):
        self.payload = ShortUint(self.class_id) + ShortUint(self.method_id) + AmqpType().join(arguments)

    @classmethod
    def decode_method(cls, frame_channel, frame_size, method_bytes):
        (class_id, _), (method_id, _), method_payload = ShortUint.decode(method_bytes[0:2]), ShortUint.decode(method_bytes[2:4]),method_bytes[4: len(method_bytes)]
        # TODO parent should not known about child do it on module level with function
        return cls.method_class_id_map[class_id.decoded_value()][method_id.decoded_value()].decode_payload(method_payload)

    @classmethod
    def decode_payload(cls, payload_bytes):
        result = []
        for i in cls.type_structure:
            # TODO use memoryview
            payload_part, payload_bytes = i.decode(payload_bytes)
            result.append(payload_part)
        return cls(arguments=result)


class Header(Frame):
    frame_type = Octet(2)
    type_structure = [ShortUint, ShortUint, LongLongUint, ShortUint, ShortString] # ShortString any times




class Content(Frame):
    frame_type = Octet(3)
    type_structure = [AmqpType]


class Heartbeat(Frame):
    frame_type = Octet(8)

    @classmethod
    def decode_method(cls, *args):
        return cls()


class Connection:
    class StartOk(Method):
        # TODO as we place it here remove sfw_interface file
        type_structure = [FieldTable, ShortString, LongString, ShortString]
        class_id = 10
        method_id = 11

    class Start(Method):
        type_structure = [Octet, Octet, FieldTable, LongString, LongString]
        class_id = 10
        method_id = 10

    class Tune(Method):
        type_structure = [ShortUint, LongUint, ShortUint]
        class_id = 10
        method_id = 30

    class TuneOk(Method):
        type_structure = [ShortUint, LongUint, ShortUint]
        class_id = 10
        method_id = 31

    class Open(Method):
        type_structure = [Path, ShortString, Octet]
        class_id = 10
        method_id = 40

    class OpenOk(Method):
        type_structure = [ShortString]
        class_id = 10
        method_id = 41


class Channel:
    class Open(Method):
        type_structure = [ShortString]
        class_id = 20
        method_id = 10

    class OpenOk(Method):
        type_structure = [ShortString]
        class_id = 20
        method_id = 11

    class Flow(Method):
        type_structure = [Octet]
        class_id = 20
        method_id = 20

    class FlowOk(Method):
        type_structure = [Octet]
        class_id = 20
        method_id = 20


class Exchange:
    class Declare(Method):
        type_structure = [ShortUint, ShortString, ShortString, Octet, FieldTable]
        class_id = 40
        method_id = 10

    class DeclareOk(Method):
        type_structure = []
        class_id = 40
        method_id = 11

class Queue:
    class Declare(Method):
        type_structure = [ShortUint, ShortString, Octet, FieldTable]
        class_id = 50
        method_id = 10

    class DeclareOk(Method):
        type_structure = [ShortString, LongUint, LongUint]
        class_id = 50
        method_id = 11


    class Bind(Method):
        type_structure = [ShortUint, ShortString, ShortString, ShortString, Octet, FieldTable]
        class_id = 50
        method_id = 20


class Basic:
    class Publish(Method):
        type_structure = [ShortUint, ShortString, ShortString, Octet]
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
            30: Connection.Tune,
            31: Connection.TuneOk,
            40: Connection.Open,
            41: Connection.OpenOk,
        },
        20: {
            10: Channel.Open,
            11: Channel.OpenOk,
            20: Channel.Flow,
            21: Channel.FlowOk
        },
        40: {
            10: Exchange.Declare,
            11: Exchange.DeclareOk
        },
        50: {
            10: Queue.Declare,
            11: Queue.DeclareOk,
            20: Queue.Bind
        },
        60: {
            40: Basic.Publish
        },
    }



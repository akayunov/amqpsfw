from amqp_types import (AmqpType, Octet, ShortUint, LongUint, FieldTable, ShortString, LongString, Char, Path,
                        LongLongUint, ExchangeName, Bit, QueueName, MessageCount, HeaderPropertyFlag, HeaderPropertyValue)
import sasl_spec
from exceptions import SfwException

# TODO do all classes byte like object to remove encode from socket.send
# TODO use memoryview, avoid too much copping


class Frame:
    frame_end = Octet(206)
    frame_type = None
    type_structure = [Octet, ShortUint, LongUint]

    def __init__(self, *args, channel_number=0):
        self._encoded = b''
        self.payload = AmqpType()
        self.channel_number = channel_number
        self.set_payload(args)
        self.encoded = (Octet(self.frame_type) + ShortUint(self.channel_number) + LongUint(len(self.payload)) + self.payload).encoded

    def set_payload(self, arguments):
        for arg, arg_type in zip(arguments, self.type_structure):
            self.payload += arg_type(arg)

    @property
    def encoded(self):
        return self._encoded + self.frame_end.encoded

    @encoded.setter
    def encoded(self, value):
        self._encoded = value

    def __str__(self):
        return str(type(self)) + ' ' + str(self.encoded)


class ProtocolHeader(Frame):
    frame_end = AmqpType('')
    type_structure = [Char, Char, Char, Char, Octet, Octet, Octet, Octet]

    def __init__(self, *args):
        self.payload = AmqpType()
        self.set_payload(args)
        self.encoded = self.payload.encoded


class Method(Frame):
    frame_type = 1
    class_id = None
    method_id = None
    type_structure = [ShortUint, ShortUint]

    def __init__(self, *args, **kwargs):
        args = [self.class_id, self.method_id] + list(args)
        self.type_structure = Method.type_structure + self.type_structure
        super().__init__(*args, ** kwargs)


class Header(Frame):
    frame_type = 2
    type_structure = [ShortUint, ShortUint, LongLongUint, HeaderPropertyFlag, HeaderPropertyValue]  # ShortString - really many bit for header


class Content(Frame):
    frame_type = 3
    type_structure = [AmqpType]


class Heartbeat(Frame):
    frame_type = 8
    type_structure = []


class Connection:
    class Start(Method):
        type_structure = [Octet, Octet, FieldTable, LongString, LongString]
        class_id = 10
        method_id = 10

    class StartOk(Method):
        type_structure = [FieldTable, ShortString, sasl_spec.Plain, ShortString]
        class_id = 10
        method_id = 11

    class Tune(Method):
        type_structure = [ShortUint, LongUint, ShortUint]
        class_id = 10
        method_id = 30

    class TuneOk(Method):
        type_structure = [ShortUint, LongUint, ShortUint]
        class_id = 10
        method_id = 31

    class Open(Method):
        type_structure = [Path, ShortString, Bit]
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
        type_structure = [LongString]
        class_id = 20
        method_id = 11

    class Flow(Method):
        type_structure = [Bit]
        class_id = 20
        method_id = 20

    class FlowOk(Method):
        type_structure = [Bit]
        class_id = 20
        method_id = 20


class Exchange:
    class Declare(Method):
        type_structure = [ShortUint, ExchangeName, ShortString, Octet, FieldTable]  # TODO Octet indeed is 5 bit
        class_id = 40
        method_id = 10

    class DeclareOk(Method):
        type_structure = []
        class_id = 40
        method_id = 11


class Queue:
    class Declare(Method):
        type_structure = [ShortUint, QueueName, Octet, FieldTable]  # TODO Octet indeed is 5 bit
        class_id = 50
        method_id = 10

    class DeclareOk(Method):
        type_structure = [QueueName, MessageCount, LongUint]
        class_id = 50
        method_id = 11

    class Bind(Method):
        type_structure = [ShortUint, QueueName, ExchangeName, ShortString, Bit, FieldTable]
        class_id = 50
        method_id = 20


class Basic:
    class Publish(Method):
        type_structure = [ShortUint, ExchangeName, ShortString, Octet]  # TODO Octet indeed is 2 bit
        class_id = 60
        method_id = 40


# TODO construct it dynamic in runtime
FRAME_TYPES = {
    1:     {
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
    },
    2: Header,
    3: Content,
    8: Heartbeat
}


def decode_frame(frame_bytes):
    if len(frame_bytes) < 7:
        return None, b''
    if frame_bytes.startswith(b'AMQP') and len(frame_bytes) > 7:
        result = []
        for i in ProtocolHeader.type_structure:
            payload_part, frame_bytes = i.decode(frame_bytes)
            result.append(payload_part.decoded_value)
        return ProtocolHeader(*result), frame_bytes[8:]

    (frame_type, _), (frame_channel, _), (frame_size, _) = Octet.decode(bytes([frame_bytes[0]])), ShortUint.decode(frame_bytes[1:3]), LongUint.decode(frame_bytes[3:7])
    if frame_size.decoded_value > len(frame_bytes[7:]) - 1:
        # data is non efficient
        return None, b''
    frame_payload, frame_end = frame_bytes[7:frame_size.decoded_value+7], frame_bytes[frame_size.decoded_value+7]
    if frame_end != frame_end:
        raise SfwException('Internal', 'Wrong frame end')
    if frame_size.decoded_value != len(frame_payload):
        raise SfwException('Internal', 'Wrong frame size')
    if frame_type != Octet(1):
        return FRAME_TYPES[frame_type.decoded_value](), frame_bytes[frame_size.decoded_value+7+1:]
    else:
        method_bytes = frame_payload
        (class_id, _), (method_id, _), method_payload = ShortUint.decode(method_bytes[0:2]), ShortUint.decode(method_bytes[2:4]), method_bytes[4: len(method_bytes)]
        payload_bytes = method_payload
        result = []
        for i in FRAME_TYPES[frame_type.decoded_value][class_id.decoded_value][method_id.decoded_value].type_structure:
            payload_part, payload_bytes = i.decode(payload_bytes)
            result.append(payload_part.decoded_value)
        return  FRAME_TYPES[frame_type.decoded_value][class_id.decoded_value][method_id.decoded_value](*result, channel_number=frame_channel.decoded_value),\
                frame_bytes[frame_size.decoded_value+7+1:]

import functools
import sys
from amqp_types import (AmqpType, Octet, ShortUint, LongUint, FieldTable, ShortString, LongString, Char, Path,
                        LongLongUint, ExchangeName, Bit, QueueName, MessageCount, HeaderPropertyFlag, HeaderPropertyValue)
import sasl_spec
from exceptions import SfwException

# TODO do all classes byte like object to remove encode from socket.send
# TODO use memoryview, avoid too much copping

THIS_MODULE = sys.modules[__name__]


def multimethod(init):
    @functools.wraps(init)
    def wrapper(*args, **kwargs):
        if len(init.__code__.co_varnames) >= len(args) + len(kwargs):
            # if argument count equal to __init__ of class, call this __init__
            init(*args, **kwargs)
        else:
            # if argument count don't equal to __init__ of class, call super __init__
            fqdn = init.__qualname__
            super(getattr(getattr(THIS_MODULE, fqdn.split('.')[0]), fqdn.split('.')[1]), args[0]).__init__(*args[1:], **kwargs)
    return wrapper


class Frame:
    frame_end = Octet(206)
    frame_type = None
    type_structure = [Octet, ShortUint, LongUint]
    dont_wait_response = 0

    def __init__(self, *args, channel_number=0):
        self._encoded = b''
        self.payload = AmqpType()
        self.channel_number = channel_number
        self.set_payload(args)
        self.encoded = (Octet(self.frame_type) + ShortUint(self.channel_number) + LongUint(len(self.payload)) + self.payload).encoded

    def set_payload(self, args):
        for arg, arg_type in zip(args, self.type_structure):
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
    dont_wait_response = 1

    @multimethod
    def __init__(self, class_id, weight=0, body_size=0, properties=None, channel_number=0):
        # property by order first property - highest bit 1000000000000000 - only first property
        properties_table = ['content-type']
        property_flag = 0
        property_values = []
        for k in properties:
            property_flag += 2 ** (15 - properties_table.index(k))
            property_values.append(properties[k])
        super().__init__(class_id, weight, body_size, channel_number=channel_number)


class Content(Frame):
    frame_type = 3
    type_structure = [AmqpType]
    dont_wait_response = 1

    @multimethod
    def __init__(self, content='', channel_number=0):
        super().__init__(content, channel_number=channel_number)


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

        @multimethod
        def __init__(self, peer_properties, mechanism, credential, locale='en_US', channel_number=0):
            super().__init__(peer_properties, mechanism, credential, locale, channel_number=channel_number)

    class Tune(Method):
        type_structure = [ShortUint, LongUint, ShortUint]
        class_id = 10
        method_id = 30

    class TuneOk(Method):
        type_structure = [ShortUint, LongUint, ShortUint]
        class_id = 10
        method_id = 31
        dont_wait_response = 1

        @multimethod
        def __init__(self, channel_max=0, frame_max=131072, heartbeat_interval=60, channel_number=0):
            super().__init__(channel_max, frame_max, heartbeat_interval, channel_number=channel_number)

    class Open(Method):
        type_structure = [Path, ShortString, Bit]
        class_id = 10
        method_id = 40

        @multimethod
        def __init__(self, virtual_host='/', channel_number=0):
            reserved1 = ''
            reserved2 = 0
            super().__init__(virtual_host, reserved1, reserved2, channel_number=channel_number)

    class OpenOk(Method):
        type_structure = [ShortString]
        class_id = 10
        method_id = 41

    class Close(Method):
        type_structure = [ShortUint, ShortString, ShortUint, ShortUint]
        class_id = 10
        method_id = 50

    class CloseOk(Method):
        type_structure = []
        class_id = 10
        method_id = 51


class Channel:
    class Open(Method):
        type_structure = [ShortString]
        class_id = 20
        method_id = 10

        @multimethod
        def __init__(self, channel_number=0):
            reserved1 = ''
            super().__init__(reserved1, channel_number=channel_number)

    class OpenOk(Method):
        type_structure = [LongString]
        class_id = 20
        method_id = 11

    class Flow(Method):
        type_structure = [Bit]
        class_id = 20
        method_id = 20

        @multimethod
        def __init__(self, active=1, channel_number=0):
            super().__init__(active, channel_number=channel_number)

    class FlowOk(Method):
        type_structure = [Bit]
        class_id = 20
        method_id = 20

    class Close(Method):
        type_structure = [ShortUint, ShortString, ShortUint, ShortUint]
        class_id = 20
        method_id = 40

    class CloseOk(Method):
        type_structure = []
        class_id = 20
        method_id = 41


class Exchange:
    class Declare(Method):
        type_structure = [ShortUint, ExchangeName, ShortString, Octet, FieldTable]
        class_id = 40
        method_id = 10

        @multimethod
        def __init__(self, exchange_name, exchange_type='topic', do_not_create=0, durable=1, auto_deleted=0, internal=0, no_wait=0, properties=None, channel_number=0):
            reserved1 = 0
            properties = {} if not properties else properties
            bits = do_not_create * 2 ** 0 + durable * 2 ** 1 + auto_deleted * 2 ** 2 + internal * 2 ** 3 + no_wait * 2 ** 4
            # <<< if bit go together when pack it in one octet
            super().__init__(reserved1, exchange_name, exchange_type, bits, properties, channel_number=channel_number)

    class DeclareOk(Method):
        type_structure = []
        class_id = 40
        method_id = 11


class Queue:
    class Declare(Method):
        type_structure = [ShortUint, QueueName, Octet, FieldTable]
        class_id = 50
        method_id = 10

        @multimethod
        def __init__(self, queue_name, passive=0, durable=1, exclusive=0, auto_deleted=0, no_wait=0, properties=None, channel_number=0):
            reserved1 = 0
            properties = {} if not properties else properties
            # TODO use <<
            bits = passive * 2 ** 0 + durable * 2 ** 1 + exclusive * 2 ** 2 + auto_deleted * 2 ** 3 + no_wait * 2 ** 4
            # <<< if bit go together when pack it in one octet
            super().__init__(reserved1, queue_name, bits, properties, channel_number=channel_number)

    class DeclareOk(Method):
        type_structure = [QueueName, MessageCount, LongUint]
        class_id = 50
        method_id = 11

    class Bind(Method):
        type_structure = [ShortUint, QueueName, ExchangeName, ShortString, Bit, FieldTable]
        class_id = 50
        method_id = 20

        @multimethod
        def __init__(self, queue_name, exchange_name, routing_key, no_wait=0, properties=None, channel_number=0):
            reserved1 = 0
            properties = {} if not properties else properties
            super().__init__(reserved1, queue_name, exchange_name, routing_key, no_wait, properties, channel_number=channel_number)

    class BindOk(Method):
        type_structure = []
        class_id = 50
        method_id = 21


class Basic:
    class Publish(Method):
        type_structure = [ShortUint, ExchangeName, ShortString, Octet]
        class_id = 60
        method_id = 40
        dont_wait_response = 1

        @multimethod
        def __init__(self, exchange_name, routing_key, mandatory=0, immediate=0, channel_number=0):
            reserved1 = 0
            bits = mandatory * 2 ** 0 + immediate * 2 ** 1
            # <<< if bit go together when pack it in one octet
            super().__init__(reserved1, exchange_name, routing_key, bits, channel_number=channel_number)


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
            50: Connection.Close,
            51: Connection.CloseOk
        },
        20: {
            10: Channel.Open,
            11: Channel.OpenOk,
            20: Channel.Flow,
            21: Channel.FlowOk,
            40: Channel.Close,
            41: Channel.CloseOk
        },
        40: {
            10: Exchange.Declare,
            11: Exchange.DeclareOk
        },
        50: {
            10: Queue.Declare,
            11: Queue.DeclareOk,
            20: Queue.Bind,
            21: Queue.BindOk
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
        return FRAME_TYPES[frame_type.decoded_value][class_id.decoded_value][method_id.decoded_value](*result, channel_number=frame_channel.decoded_value),\
               frame_bytes[frame_size.decoded_value+7+1:]

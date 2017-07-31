import struct


# ========= types ============
class AmqpType:
    def __init__(self, data=b''):
        self.encoded = data
        self.parts = []  # TODO weakref

    def __add__(self, other):
        result = AmqpType()
        if hasattr(other, 'encoded'):
            result.encoded = self.encoded + other.encoded
            result.parts = self.parts + other.parts
        else:
            result.encoded = self.encoded + other
            result.parts = self.parts + [other]
        return result

    def __iadd__(self, other):
        result = AmqpType()
        if hasattr(other, 'encoded'):
            result.encoded = self.encoded + other.encoded
            result.parts = self.parts + other.parts
        else:
            result.encoded = self.encoded + other
            result.parts = self.parts + [other]
        return result

    def __len__(self):
        return len(self.encoded)

    def join(self, array):
        result = AmqpType()
        for i in array:
            result = result + i
            if hasattr(i, 'parts'):
                result.parts += i.parts
            else:
                result.parts += i
        return result

    def __str__(self):
        return str(type(self)) + ' ' + str(self.encoded)

    def __repr__(self):
        return str(type(self)) + ' ' + str(self.encoded)


class ShortString(AmqpType):
    def __init__(self, string_data):
        super().__init__()
        string_bytes = string_data.encode('utf8')
        self.encoded = struct.pack('B', len(string_bytes)) + string_bytes


class LongString(AmqpType):
    def __init__(self, string_data):
        super().__init__()
        if type(string_data) in [bytes, bytearray]:
            string_bytes = string_data
        elif type(string_data) is AmqpType:
            string_bytes = string_data.encoded
        else:
            string_bytes = string_data.encode('utf8')
        self.encoded = struct.pack('!l', len(string_bytes)) + string_bytes


class Octet(AmqpType):
    def __init__(self, integer_data):
        super().__init__()
        self.encoded = struct.pack('B', integer_data)


Bool = Octet


class ShortUint(AmqpType):
    def __init__(self, integer_data):
        super().__init__()
        self.encoded = struct.pack('!H', integer_data)


class LongUint(AmqpType):
    def __init__(self, integer_data):
        super().__init__()
        self.encoded = struct.pack('!l', integer_data)


class LongLongUint(AmqpType):
    def __init__(self, integer_data):
        super().__init__()
        self.encoded = struct.pack('!Q', integer_data)


class FieldTable(AmqpType):
    amqp_types = {
        Bool: Octet(ord('t')),
        # short sting does not parsed - strage!!
        LongString: Octet(ord('S'))
    }

    def __init__(self, dict_data):
        super().__init__()
        # for example dict_data = {'field_name1': LongString, 'field_name2': Octet, 'filed_name3': ShortString}
        result = AmqpType()
        for field_name in dict_data:
            result += ShortString(field_name) + FieldTable.amqp_types[type(dict_data[field_name])] + dict_data[field_name]
        self.encoded = (LongUint(len(result)) + result).encoded

# ========= types ============
# ========= frames =============

class ProtocolHeader:
    def __init__(self):
        self.encoded = struct.pack('ccccBBBB', b'A', b'M', b'Q', b'P', 0, 0, 9, 1)


class Frame:
    frame_end = Octet(206)

    def __init__(self):
        self._encoded = AmqpType().encoded

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
    class_id = Octet(1)
    method_id = Octet(1)
    channel_number = Octet(0)

    def __init__(self, arguments):
        payload = self.class_id + self.method_id + AmqpType().join(arguments)
        super().__init__()
        self.encoded = (self.frame_type + self.channel_number + LongUint(len(payload)) + payload).encoded


class Header(Frame):
    frame_type = Octet(2)

    def __init__(self, channel_number=ShortUint(0), arguments=''):
        self.channel_number = channel_number
        payload = AmqpType().join(arguments)
        super().__init__()
        self.encoded = (self.frame_type + self.channel_number + LongUint(len(payload)) + payload).encoded


class Content(Frame):
    frame_type = Octet(3)

    def __init__(self, channel_number=ShortUint(0), data=''):
        self.channel_number = channel_number
        payload = AmqpType(data.encode('utf8'))
        super().__init__()
        self.encoded = (self.frame_type + self.channel_number + LongUint(len(payload)) + payload).encoded


class Heartbeat(Frame):
    frame_type = Octet(8)
    channel_number = ShortUint(0)

    def __init__(self):
        payload = AmqpType()
        super().__init__()
        self.encoded = (self.frame_type + self.channel_number + LongUint(len(payload)) + payload).encoded


class Connection:
    class StartOk(Method):
        channel_number = ShortUint(0)
        class_id = ShortUint(10)
        method_id = ShortUint(11)

    class TuneOk(Method):
        channel_number = ShortUint(0)
        class_id = ShortUint(10)
        method_id = ShortUint(31)

    class Open(Method):
        channel_number = ShortUint(0)
        class_id = ShortUint(10)
        method_id = ShortUint(40)


class Channel:
    class Open(Method):
        channel_number = ShortUint(1)
        class_id = ShortUint(20)
        method_id = ShortUint(10)

    class Flow(Method):
        channel_number = ShortUint(1)
        class_id = ShortUint(20)
        method_id = ShortUint(20)


class Exchange:
    class Declare(Method):
        channel_number = ShortUint(1)
        class_id = ShortUint(40)
        method_id = ShortUint(10)


class Queue:
    class Declare(Method):
        channel_number = ShortUint(1)
        class_id = ShortUint(50)
        method_id = ShortUint(10)

    class Bind(Method):
        channel_number = ShortUint(1)
        class_id = ShortUint(50)
        method_id = ShortUint(20)


class Basic:
    class Publish(Method):
        channel_number = ShortUint(1)
        class_id = ShortUint(60)
        method_id = ShortUint(40)

# ========= frames =============

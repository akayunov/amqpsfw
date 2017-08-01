import struct


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

    def __eq__(self, other):
        return self.encoded == other.encoded


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

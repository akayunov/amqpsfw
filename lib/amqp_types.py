import struct


class AmqpType:
    def __init__(self, data=b''):
        self.encoded = data
        self.parts = [type(self)] # do we really need this?

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
                result.parts += type(i)
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

    @classmethod
    def get_len(cls, binary_data):
        return struct.unpack('B', bytes([binary_data[0]]))[0] + 1

    @classmethod
    def decode(cls, binary_data):
        return cls(binary_data[1:].decode('utf8'))

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

    @classmethod
    def get_len(cls, binary_data):
        return struct.unpack('!l', binary_data[0:4])[0] + 4

    @classmethod
    def decode(cls, binary_data):
        return cls(binary_data[1:].decode('utf8'))

class Char(AmqpType):
    def __init__(self, symbol):
        super().__init__()
        self.encoded = struct.pack('c', symbol)

    @classmethod
    def decode(cls, binary_data):
        return struct.unpack('c', binary_data)[0]

    @classmethod
    def get_len(cls, binary_data=None):
        return 1

class Octet(AmqpType):
    def __init__(self, integer_data):
        super().__init__()
        self.integer_data = integer_data
        self.encoded = struct.pack('B', integer_data)

    @classmethod
    def decode(cls, binary_data):
        return cls(struct.unpack('B', binary_data)[0])

    def decoded_value(self):
        return self.integer_data

    @classmethod
    def get_len(cls, binary_data=None):
        return 1


Bool = Octet


class ShortUint(AmqpType):
    def __init__(self, integer_data):
        super().__init__()
        self.encoded = struct.pack('!H', integer_data)

    @classmethod
    def decode(cls, binary_data):
        return struct.unpack('!H', binary_data)[0]

    @classmethod
    def get_len(cls, binary_data=None):
        return 2


class LongUint(AmqpType):
    def __init__(self, integer_data):
        super().__init__()
        self.encoded = struct.pack('!l', integer_data)

    @classmethod
    def decode(cls, binary_data):
        return struct.unpack('!l', binary_data)[0]

    @classmethod
    def get_len(cls, binary_data=None):
        return 4


class LongLongUint(AmqpType):
    def __init__(self, integer_data):
        super().__init__()
        self.encoded = struct.pack('!Q', integer_data)

    @classmethod
    def get_len(cls, binary_data=None):
        return 8


class FieldTable(AmqpType):
    amqp_types = {
        Bool: Octet(ord('t')),
        # short sting does not parsed - strage!!
        LongString: Octet(ord('S'))
    }

    amqp_types_ = {
        'S': LongString
    }

    def __init__(self, dict_data):
        super().__init__()
        # for example dict_data = {'field_name1': LongString, 'field_name2': Octet, 'filed_name3': ShortString}
        result = AmqpType()
        for field_name in dict_data:
            result += ShortString(field_name) + FieldTable.amqp_types[type(dict_data[field_name])] + dict_data[field_name]
        self.encoded = (LongUint(len(result)) + result).encoded

    @classmethod
    def get_len(cls, binary_data):

        return struct.unpack('!l', binary_data[0:4])[0] + 4

    @classmethod
    def decode(cls, binary_data):
        table_size, binary_data = struct.unpack('!l', binary_data[0:4]), binary_data[4:]
        #while len(binary_data):
        # k_len = ShortString.get_len(binary_data)
        # k_size, k_data, v_type, binary_data = binary_data[0], binary_data[0: k_len], binary_data[k_len + 1], binary_data[k_len+1:]
        # v_len = cls.amqp_types_[v_type].get_len(binary_data)
        # v_size, v_data = binary_data[0], binary_data[0: k_len]
        # # table_size, binary_data = struct.unpack('B', binary_data[0:s_len]), binary_data[s_len:]
        return FieldTable({'host': LongString('localhost')})

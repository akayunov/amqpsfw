import struct

# TODO use memory view on slicing
# TODO it is just sfwtypes not AMQP because it used in sasl module


class AmqpType:

    def __init__(self, data=''):
        self.encoded = b''
        self.decoded_value = data
        self.parts = [type(self)]  # TODO do we really need this?

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

# # TODO do we really need this arter new argument parser???
#     @staticmethod
#     def join(array):
#         result = AmqpType()
#         for i in array:
#             result = result + i
#             if hasattr(i, 'parts'):
#                 result.parts += i.parts
#             else:
#                 result.parts += type(i)
#         return result

    def __str__(self):
        return str(type(self)) + ' ' + str(self.encoded)

    def __repr__(self):
        return str(type(self)) + ' ' + str(self.encoded)

    def __eq__(self, other):
        return self.encoded == other.encoded


class String(AmqpType):
    def __init__(self, string_data):
        super().__init__(string_data)
        self.encoded = string_data.encode('utf8')

    @classmethod
    def decode(cls, binary_data):
        return cls(binary_data.decode('utf8')), binary_data[len(binary_data) + 1:]


class ShortString(AmqpType):
    def __init__(self, string_data):
        super().__init__(string_data)
        string_bytes = string_data.encode('utf8')
        self.encoded = struct.pack('B', len(string_bytes)) + string_bytes

    @classmethod
    def decode(cls, binary_data):
        lenght = struct.unpack('B', bytes([binary_data[0]]))[0]
        return cls(binary_data[1:lenght + 1].decode('utf8')), binary_data[lenght + 1:]


class ConsumerTag(ShortString):
    pass


class Path(ShortString):
    pass


class QueueName(ShortString):
    pass


class LongString(AmqpType):
    def __init__(self, string_data):
        super().__init__(string_data)
        if type(string_data) in [bytes, bytearray]:
            string_bytes = string_data
        elif type(string_data) is AmqpType:
            string_bytes = string_data.encoded
        else:
            string_bytes = string_data.encode('utf8')
        self.encoded = struct.pack('!l', len(string_bytes)) + string_bytes

    @classmethod
    def decode(cls, binary_data):
        lenght = struct.unpack('!l', binary_data[0:4])[0]
        return cls(binary_data[4:4+lenght].decode('utf8')), binary_data[lenght+4:]


class Char(AmqpType):
    def __init__(self, symbol):
        super().__init__(symbol)
        self.encoded = struct.pack('c', symbol.encode('utf8'))

    @classmethod
    def decode(cls, binary_data):
        return cls(struct.unpack('c', bytes([binary_data[0]]))[0].decode('utf8')), binary_data[1:]


class Octet(AmqpType):
    def __init__(self, integer_data):
        super().__init__(integer_data)
        self.encoded = struct.pack('B', integer_data)

    @classmethod
    def decode(cls, binary_data):
        return cls(struct.unpack('B', bytes([binary_data[0]]))[0]), binary_data[1:]


class Bool(Octet):
    pass


class Bit(Octet):
    pass


class ShortUint(AmqpType):
    def __init__(self, integer_data):
        super().__init__(integer_data)
        self.encoded = struct.pack('!H', integer_data)

    @classmethod
    def decode(cls, binary_data):
        return cls(struct.unpack('!H', binary_data[:2])[0]), binary_data[2:]


class ExchangeName(ShortString):
    pass


class HeaderPropertyFlag(ShortUint):
    pass


class HeaderPropertyValue(AmqpType):
    # TODO may be different types see basic publish header property
    # TODO realize it
    def __init__(self, string_array):
        super().__init__(string_array)
        self.encoded = b''.join([ShortString(i).encoded for i in string_array])

    @classmethod
    def decode(cls, binary_data):
        string_array = []
        while binary_data:
            string_element, binary_data = ShortString.decode(binary_data)
            string_array.append(string_element.decoded_value)
        return cls(string_array), binary_data


class LongUint(AmqpType):
    def __init__(self, integer_data):
        super().__init__(integer_data)
        self.encoded = struct.pack('!l', integer_data)

    @classmethod
    def decode(cls, binary_data):
        return cls(struct.unpack('!l', binary_data[:4])[0]), binary_data[4:]


class MessageCount(LongUint):
    pass


class LongLongUint(AmqpType):
    def __init__(self, integer_data):
        super().__init__(integer_data)
        self.encoded = struct.pack('!Q', integer_data)

    @classmethod
    def decode(cls, binary_data):
        return cls(struct.unpack('!Q', binary_data[:8])[0]), binary_data[8:]


class DeliveryTag(LongLongUint):
    pass


class FieldTable(AmqpType):
    def __init__(self, dict_data):
        super().__init__(dict_data)
        # for example dict_data = {'field_name1': ['S' : 'value1'], 'field_name2': ['t': value2]...}
        result = AmqpType()
        for field_name in dict_data:
            result += ShortString(field_name) + Char(dict_data[field_name][0]) + amqp_types_code_to_type[dict_data[field_name][0]](dict_data[field_name][1])
        self.encoded = (LongUint(len(result)) + result).encoded

    @classmethod
    def decode(cls, binary_data):
        length = struct.unpack('!l', binary_data[0:4])[0]
        table = binary_data[4:length + 4]
        result = {}
        while len(table):
            key, table = ShortString.decode(table)
            v_type, table = Char.decode(table)
            v_value, table = amqp_types_code_to_type[v_type.decoded_value].decode(table)
            result[key.decoded_value] = [v_type.decoded_value, v_value.decoded_value]
        return cls(result), binary_data[length + 4:]

amqp_types_code = {
    Bool: Char('t'),
    # short sting does not parsed - strage!!
    LongString: Char('S'),
    Octet: Char('B'),
    FieldTable: Char('F'),
}

amqp_types_code_to_type = {
    # short sting does not parsed - strage!!
    'S': LongString,
    'F': FieldTable,
    't': Octet
}

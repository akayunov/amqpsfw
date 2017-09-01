import struct

from amqpsfw.exceptions import SfwException


class AmqpType:
    def __init__(self, data):
        self.encoded = b''
        self.decoded_value = data

    @classmethod
    def decode(cls, binary_data):
        raise NotImplementedError

    def __add__(self, other):
        result = AmqpType('')
        if hasattr(other, 'encoded'):
            result.encoded = self.encoded + other.encoded
        else:
            result.encoded = self.encoded + other
        return result

    def __iadd__(self, other):
        result = AmqpType('')
        if hasattr(other, 'encoded'):
            result.encoded = self.encoded + other.encoded
        else:
            result.encoded = self.encoded + other
        return result

    def __len__(self):
        return len(self.encoded)

    def __str__(self):
        return str(type(self)) + ' ' + str(self.encoded)

    def __repr__(self):
        return str(type(self)) + ' ' + str(self.encoded)

    def __eq__(self, other):
        return self.encoded == other.encoded


class Char(AmqpType):
    def __init__(self, symbol):
        super().__init__(symbol)
        self.encoded = struct.pack('c', symbol.encode('utf8'))

    @classmethod
    def decode(cls, binary_data):
        return cls(struct.unpack('c', binary_data[0:1])[0].decode('utf8')), binary_data[1:]


class String(AmqpType):
    def __init__(self, string_data):
        super().__init__(string_data)
        if type(string_data) in [bytes, bytearray]:
            string_bytes = string_data
        elif issubclass(type(string_data), (AmqpType,)):
            string_bytes = string_data.encoded
        else:
            string_bytes = string_data.encode('utf8')
        self.encoded = string_bytes

    @classmethod
    def decode(cls, binary_data):
        return cls(binary_data.decode('utf8')), b''


class ShortString(AmqpType):
    def __init__(self, string_data):
        super().__init__(string_data)
        string_bytes = string_data.encode('utf8')
        length = len(string_bytes)
        self.encoded = struct.pack('B', length) + string_bytes

    @classmethod
    def decode(cls, binary_data):
        lenght = struct.unpack('B', binary_data[0:1])[0]
        return cls(binary_data[1:lenght + 1].decode('utf8')), binary_data[lenght + 1:]


class ConsumerTag(ShortString):
    pass


class Path(ShortString):
    pass


class QueueName(ShortString):
    pass


class ExchangeName(ShortString):
    pass


class LongString(AmqpType):
    def __init__(self, string_data):
        super().__init__(string_data)
        if type(string_data) in [bytes, bytearray]:
            string_bytes = string_data
        elif issubclass(type(string_data), (AmqpType,)):
            string_bytes = string_data.encoded
        else:
            string_bytes = string_data.encode('utf8')
        length = len(string_bytes)
        self.encoded = struct.pack('!L', length) + string_bytes

    @classmethod
    def decode(cls, binary_data):
        lenght = struct.unpack('!L', binary_data[0:4])[0]
        return cls(binary_data[4:4 + lenght].decode('utf8')), binary_data[lenght + 4:]


class Bool(AmqpType):
    def __init__(self, bool_data):
        super().__init__(bool_data)
        self.encoded = struct.pack('?', bool_data)

    @classmethod
    def decode(cls, binary_data):
        return cls(struct.unpack('?', binary_data[0:1])[0]), binary_data[1:]


class Bit1(AmqpType):
    length = 1

    def __init__(self, integers_array):
        super().__init__(integers_array[:self.length])
        r = 0
        for i, v in enumerate(integers_array):
            r += v << i
        self.encoded = struct.pack('B', r)

    @classmethod
    def decode(cls, binary_data):
        integers_array = list(reversed(list(map(int, list(bin(struct.unpack('B', binary_data[0:1])[0])[2:].zfill(8))))))
        return cls(integers_array), binary_data[1:]


class Bit2(Bit1):
    length = 2


class Bit3(Bit1):
    length = 3


class Bit4(Bit1):
    length = 4


class Bit5(Bit1):
    length = 5


class Bit6(Bit1):
    length = 6


class Bit7(Bit1):
    length = 7


class Bit8(Bit1):
    length = 8


class ShortShortInt(AmqpType):
    def __init__(self, integer_data):
        super().__init__(integer_data)
        self.encoded = struct.pack('b', integer_data)

    @classmethod
    def decode(cls, binary_data):
        return cls(struct.unpack('b', binary_data[0:1])[0]), binary_data[1:]


class ShortShortUint(AmqpType):
    def __init__(self, integer_data):
        super().__init__(integer_data)
        self.encoded = struct.pack('B', integer_data)

    @classmethod
    def decode(cls, binary_data):
        return cls(struct.unpack('B', binary_data[0:1])[0]), binary_data[1:]


class ShortInt(AmqpType):
    def __init__(self, integer_data):
        super().__init__(integer_data)
        self.encoded = struct.pack('!h', integer_data)

    @classmethod
    def decode(cls, binary_data):
        return cls(struct.unpack('!h', binary_data[:2])[0]), binary_data[2:]


class ShortUint(AmqpType):
    def __init__(self, integer_data):
        super().__init__(integer_data)
        self.encoded = struct.pack('!H', integer_data)

    @classmethod
    def decode(cls, binary_data):
        return cls(struct.unpack('!H', binary_data[:2])[0]), binary_data[2:]


class LongInt(AmqpType):
    def __init__(self, integer_data):
        super().__init__(integer_data)
        self.encoded = struct.pack('!l', integer_data)

    @classmethod
    def decode(cls, binary_data):
        return cls(struct.unpack('!l', binary_data[:4])[0]), binary_data[4:]


class LongUint(AmqpType):
    def __init__(self, integer_data):
        super().__init__(integer_data)
        self.encoded = struct.pack('!L', integer_data)

    @classmethod
    def decode(cls, binary_data):
        return cls(struct.unpack('!L', binary_data[:4])[0]), binary_data[4:]


class MessageCount(LongUint):
    pass


class LongLongInt(AmqpType):
    def __init__(self, integer_data):
        super().__init__(integer_data)
        self.encoded = struct.pack('!q', integer_data)

    @classmethod
    def decode(cls, binary_data):
        return cls(struct.unpack('!q', binary_data[:8])[0]), binary_data[8:]


class LongLongUint(AmqpType):
    def __init__(self, integer_data):
        super().__init__(integer_data)
        self.encoded = struct.pack('!Q', integer_data)

    @classmethod
    def decode(cls, binary_data):
        return cls(struct.unpack('!Q', binary_data[:8])[0]), binary_data[8:]


class DeliveryTag(LongLongUint):
    pass


class Float(AmqpType):
    def __init__(self, float_data):
        super().__init__(float_data)
        self.encoded = struct.pack('!f', float_data)

    @classmethod
    def decode(cls, binary_data):
        return cls(struct.unpack('!f', binary_data[:4])[0]), binary_data[4:]


class Double(AmqpType):
    def __init__(self, double_data):
        super().__init__(double_data)
        self.encoded = struct.pack('!d', double_data)

    @classmethod
    def decode(cls, binary_data):
        return cls(struct.unpack('!d', binary_data[:8])[0]), binary_data[8:]


class Decimal(AmqpType):
    # TODO implement
    def __init__(self, decimal_data):
        super().__init__(decimal_data)
        if '.' in decimal_data:
            number_count_after_comma = str(decimal_data).split('.')[1]
        else:
            number_count_after_comma = 0
        self.encoded = (ShortShortInt(number_count_after_comma) + LongUint(decimal_data)).encoded

    @classmethod
    def decode(cls, binary_data):
        number_count_after_comma, binary_data = ShortShortInt.decode(binary_data)
        value, binary_data = LongUint.decode(binary_data)
        return cls(value.decoded_value), binary_data[5:]


class TimeStamp(LongLongUint):
    pass


class FieldArray(AmqpType):
    def __init__(self, array_data):
        super().__init__(array_data)
        result = AmqpType('')
        if type(array_data) is not list:
            raise SfwException('Internal', 'Input type for FieldArray object is wrong')
        if not array_data:
            pass
        else:
            for li in array_data:
                result += Char(get_suitable_type(li)) + amqp_mapping[get_suitable_type(li)](li)
        self.encoded = (LongInt(len(result)) + result).encoded

    @classmethod
    def decode(cls, binary_data):
        length = struct.unpack('!l', binary_data[0:4])[0]
        table = binary_data[4:length + 4]
        result = []
        while len(table):
            v_type, table = Char.decode(table)
            v_value, table = amqp_mapping[v_type.decoded_value].decode(table)
            result.append(v_value.decoded_value)
        return cls(result), binary_data[length + 4:]


class FieldTable(AmqpType):
    def __init__(self, dict_data):
        super().__init__(dict_data)
        result = AmqpType('')
        if not dict_data:
            pass
        else:
            if type(dict_data) is not dict:
                raise SfwException('Internal', 'Input type for FieldTable object is wrong')
            for field_name in dict_data:
                t = ShortString(field_name) + Char(get_suitable_type(dict_data[field_name])) + amqp_mapping[get_suitable_type(dict_data[field_name])](dict_data[field_name])
                result += t
        self.encoded = (LongUint(len(result)) + result).encoded

    @classmethod
    def decode(cls, binary_data):
        length = struct.unpack('!l', binary_data[0:4])[0]
        table = binary_data[4:length + 4]
        result = {}
        while len(table):
            key, table = ShortString.decode(table)
            v_type, table = Char.decode(table)
            v_value, table = amqp_mapping[v_type.decoded_value].decode(table)
            result[key.decoded_value] = v_value.decoded_value
        return cls(result), binary_data[length + 4:]


def get_suitable_type(obj):
    if type(obj) is str:
        if len(obj) < 256:
            # TODO short string doesn't parsed by rabbitmq
            return 'S'
        else:
            return 'S'
    elif type(obj) is int:
        if obj < 128:
            return 'b'
        elif obj < 256:
            return 'B'
        elif obj < 32768:
            return 'U'
        elif obj < 65536:
            return 'u'
        elif obj < 2147483648:
            return 'I'
        elif obj < 4294967296:
            return 'i'
        else:
            return 'L'
    elif type(obj) is float:
        return 'f'
    elif type(obj) is bool:
        return 't'
    elif type(obj) is dict:
        return 'F'
    elif type(obj) is list:
        return 'A'
    else:
        raise SfwException('Internal', 'Unknown type for filedtable type: ' + str(obj))


amqp_mapping = {
    't': Bool,
    'b': ShortShortInt,
    'B': ShortShortUint,
    'U': ShortInt,
    'u': ShortUint,
    'I': LongInt,
    'i': LongUint,
    'L': LongLongInt,
    'l': LongLongUint,
    'f': Float,
    'd': Double,
    'D': Decimal,
    's': ShortString,
    'S': LongString,
    'A': FieldArray,
    'T': TimeStamp,
    'F': FieldTable
}


class Reserved(AmqpType):
    @classmethod
    def decode(cls, binary_data):
        raise NotImplementedError


class ReservedShortString(ShortString, Reserved):
    pass


class ReservedLongString(LongString, Reserved):
    pass


class ReservedBit1(Bit1, Reserved):
    pass


class ReservedShortUint(ShortUint, Reserved):
    pass


class HeaderProperty(AmqpType):
    properties_table = ['content-type', 'content­encoding', 'headers', 'delivery­mode', 'priority', 'correlation­id', 'reply­to', 'expiration',
                        'message­id', 'timestamp', 'type', 'user­id', 'app­id', 'reserved1', 'reserved2', 'reserved3']
    properties_types = [ShortString, ShortString, FieldTable, ShortShortInt, ShortShortInt, ShortString, ShortString, ShortString, ShortString, LongLongUint, ShortString,
                        ShortString, ShortString,
                        ShortString, AmqpType, AmqpType]

    def __init__(self, properties):
        super().__init__(properties)
        properties = dict(sorted(properties.items(), key=lambda x: self.properties_table.index(x[0])))
        property_flag = 0
        result = AmqpType('')

        for i, k in enumerate(properties):
            index = list(reversed(self.properties_table)).index(k)
            property_flag += 1 << index
            result += list(reversed(self.properties_types))[index](properties[k])
        self.encoded = (ShortUint(property_flag) + result).encoded

    @classmethod
    def decode(cls, binary_data):
        property_flag, binary_data = ShortUint.decode(binary_data)
        integers_array = list(map(int, list(bin(property_flag.decoded_value)[2:].zfill(16))))
        properties = {}
        for index, value in enumerate(integers_array):
            if value == 0:
                continue
            else:
                string_element, binary_data = cls.properties_types[index].decode(binary_data)
                properties[cls.properties_table[index]] = string_element.decoded_value
        return cls(properties), binary_data

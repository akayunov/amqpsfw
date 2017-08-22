import struct

from amqpsfw.exceptions import SfwException


# TODO use memory view on slicing
# TODO it is just sfwtypes not AMQP because it used in sasl module


class AmqpType:
    def __init__(self, data=''):
        self.encoded = b''
        self.decoded_value = data

    @classmethod
    def decode(cls, binary_data):
        raise NotImplementedError

    def __add__(self, other):
        result = AmqpType()
        if hasattr(other, 'encoded'):
            result.encoded = self.encoded + other.encoded
        else:
            result.encoded = self.encoded + other
        return result

    def __iadd__(self, other):
        result = AmqpType()
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


class String(AmqpType):
    def __init__(self, string_data):
        super().__init__(string_data)
        self.encoded = string_data.encode('utf8')

    @classmethod
    def decode(cls, binary_data):
        return cls(binary_data.decode('utf8')), b''


class Reserved(AmqpType):
    pass


class ShortString(AmqpType):
    def __init__(self, string_data):
        super().__init__(string_data)
        string_bytes = string_data.encode('utf8')
        length = len(string_bytes)
        self.encoded = struct.pack('B', length) + string_bytes

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
        length = len(string_bytes)
        self.encoded = struct.pack('!l', length) + string_bytes

    @classmethod
    def decode(cls, binary_data):
        lenght = struct.unpack('!l', binary_data[0:4])[0]
        return cls(binary_data[4:4 + lenght].decode('utf8')), binary_data[lenght + 4:]


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


# TODO fix it


class Bit1(AmqpType):
    length = 1

    def __init__(self, integers_array):
        # TODO use <<
        integers_array.reverse()
        super().__init__(integers_array)
        self.encoded = struct.pack('B', int(''.join([str(i) for i in integers_array]), base=2))

    @classmethod
    def decode(cls, binary_data):
        # TODO use <<
        # TODO use bin() instead of bytes???
        qwe = list(str(bin(struct.unpack('B', bytes([binary_data[0]]))[0])).split('b')[1])
        qwe = [int(i) for i in qwe]
        integers_array = ([0, 0, 0, 0, 0] + qwe)[-1::-1][:cls.length]
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


class ShortUint(AmqpType):
    def __init__(self, integer_data):
        super().__init__(integer_data)
        self.encoded = struct.pack('!H', integer_data)

    @classmethod
    def decode(cls, binary_data):
        return cls(struct.unpack('!H', binary_data[:2])[0]), binary_data[2:]


class ExchangeName(ShortString):
    pass


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
        if dict_data is None:
            pass
        else:
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
                        'message­id', 'timestamp', 'type', 'user­id', 'app­id', 'reserved']
    properties_types = [ShortString, ShortString, FieldTable, Octet, Octet, ShortString, ShortString, ShortString, ShortString, LongLongUint, ShortString, ShortString, ShortString,
                        ShortString]

    def __init__(self, properties):
        super().__init__(properties)
        # property by order first property - highest bit 1000000000000000 - only first property
        # properties = {'content-type': 'application/json'}
        property_flag = 0
        result = AmqpType()

        for k in properties:
            # TODO use <<
            index = self.properties_table.index(k)
            property_flag += 2 ** (15 - index)
            result += self.properties_types[index](properties[k])
        self.encoded = (ShortUint(property_flag) + result).encoded

    @classmethod
    def decode(cls, binary_data):
        property_flag, binary_data = ShortUint.decode(binary_data)
        # TODO use <<
        qwe = str(bin(int(property_flag.decoded_value))).split('b')[1]
        properties = {}
        for index, value in enumerate(qwe):
            if value == '0':
                continue
            else:
                string_element, binary_data = cls.properties_types[index].decode(binary_data)
                properties[cls.properties_table[index]] = string_element.decoded_value
        return cls(properties), binary_data


amqp_types_code = {
    Bool: Char('t'),
    # short sting does not parsed - strage!!
    LongString: Char('S'),
    Octet: Char('B'),
    FieldTable: Char('F'),
}
# TODO just reverse above
amqp_types_code_to_type = {
    # short sting does not parsed - strage!!
    'S': LongString,
    'F': FieldTable,
    't': Octet
}

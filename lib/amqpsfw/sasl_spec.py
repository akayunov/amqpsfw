from amqpsfw.amqp_types import AmqpType, ShortShortUint, String, LongString


class Plain(AmqpType):
    def __init__(self, string_array):
        super().__init__(string_array)
        result = AmqpType('')
        for string in string_array:
            result += ShortShortUint(0)
            result += String(string)
        self.encoded = LongString(result).encoded

    @classmethod
    def decode(cls, binary_data):
        sasl_string, binary_data = LongString.decode(binary_data)
        string_array = []
        string = ''
        for current_byte in sasl_string.decoded_value:
            if current_byte == '\x00':
                if string:
                    string_array.append(string)
                    string = ''
            else:
                string += current_byte
        string_array.append(string)
        return cls(string_array), binary_data


import struct
from amqp_types import (AmqpType, Octet, String, LongUint, FieldTable, ShortString, LongString, Char, Path,
                        LongLongUint, ExchangeName, Bit, QueueName, MessageCount)
from exceptions import SfwException


class Plain(AmqpType):
    type_structure = [String, String]

    def __init__(self, *args):
        super().__init__()
        self.payload = AmqpType()
        self.set_payload(*args)
        self.encoded = self.payload.encoded

    def set_payload(self, arguments):
        for arg, arg_type in zip(arguments, self.type_structure):
            self.payload += Octet(0)
            self.payload += arg_type(arg)
        self.payload = LongString(AmqpType('PLAIN') + self.payload)

    @property
    def encoded(self):
        return self._encoded

    @encoded.setter
    def encoded(self, value):
        self._encoded = value

    def __str__(self):
        return str(type(self)) + ' ' + str(self.encoded)


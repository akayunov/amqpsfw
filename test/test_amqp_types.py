import decimal
import os
import sys

import pytest

sys.path = [os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib')] + sys.path

from amqpsfw.amqp_types import (AmqpType, String, Reserved, ShortString, ConsumerTag, Path, QueueName, LongString, Char, Bool, Bit1, Bit2, Bit3, Bit4, Bit5, Bit6, Bit7,
                                Bit8, ShortUint, ExchangeName, HeaderProperty, LongUint, MessageCount, LongLongUint, DeliveryTag, FieldTable, ReservedShortString,
                                ReservedLongString, ReservedBit1, ReservedShortUint, ShortShortUint, ShortInt, LongInt, Float, Double, Decimal, LongLongInt, FieldArray,
                                TimeStamp, ShortShortInt)

# TODO test complex type Header FiledArray FiledTAble
# TODO add coverage


class TestAmqpTypes:
    def test_amqp_len(self):
        length = 5
        obj = String('q' * length)
        assert len(obj) == length

    def test_amqp_add_and_equality(self):
        text1 = 'qwe'
        text2 = 'rty'
        assert String(text1) + String(text2) == String(text1 + text2)

    # TODO test that decode return tail of bytes is rigth
    @pytest.mark.parametrize('types', [
        (String, 'qwqwr'), (ShortString, 'asfaf'), (ConsumerTag, 'zvzdbv'), (Path, 'asdggas'), (QueueName, 'sdgsdg'), (ExchangeName, 'asagaga'), (LongString, 'asgfagdsag'),
        (LongString, bytes([1, 2, 3, 15])), (LongString, bytearray([1, 2, 3, 4])), (LongString, String('aqwrq')), (Char, 'c'),
        (Bool, True), (Bit1, [1]), (Bit2, [1, 1]), (Bit3, [1, 0, 1]), (Bit4, [1, 0, 1, 1]), (Bit5, [1, 1, 1, 0, 1]), (Bit6, [1, 0, 0, 0, 0, 1]), (Bit7, [1, 0, 0, 0, 0, 0, 1]),
        (Bit8, [1, 1, 1, 1, 1, 0, 0, 1]), (ShortUint, 124), (ShortShortUint, 12), (ShortInt, 12), (LongInt, 125), (ShortShortInt, 125),
        (HeaderProperty,
         dict(zip(HeaderProperty.properties_table, ['application/json', 'identity', dict(), 1, 2,
                                                    'asfa', 'asfasf', 'zxvz', 'gjfj', 1503404274,
                                                    'asfagf', 'dg', 'vncnv', 'afa']))
         ), (LongUint, 12415454), (MessageCount, 12), (LongLongUint, 12414), (LongLongInt, 124), (DeliveryTag, 124124), (Float, 123.142), (Double, 121.4124),
        # TODO (Decimal, decimal.Decimal(12414.12414)),
        (TimeStamp, 1241), (FieldArray, ['qwe', True, 123, {'asd': 1.23}]),
        (FieldTable, {'host': 'localhost', 'qw': 2.1, 'asf': {'xc': ['qwe', 'asd', 214, []]}}), (ReservedShortString, 'asfasf'),
        (ReservedLongString, 'asfsags'), (ReservedBit1, [1]), (ReservedShortUint, 1241)
    ])
    def test_encode_decode_positive(self, types):
        amqp_type, value = types
        obj = amqp_type(value)
        assert (obj, b'') == amqp_type.decode(obj.encoded)

    @pytest.mark.parametrize('types', [(AmqpType, ''), (Reserved, '')])
    def test_encode_decode_negative(self, types):
        amqp_type, value = types
        obj = amqp_type(value)
        with pytest.raises(NotImplementedError):
            amqp_type.decode(obj.encoded)



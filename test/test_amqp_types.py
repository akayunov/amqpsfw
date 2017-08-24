import decimal
import os
import sys

import pytest

sys.path = [os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib')] + sys.path

from amqpsfw.exceptions import SfwException
from amqpsfw.amqp_types import (AmqpType, String, Reserved, ShortString, ConsumerTag, Path, QueueName, LongString, Char, Bool, Bit1, Bit2, Bit3, Bit4, Bit5, Bit6, Bit7,
                                Bit8, ShortUint, ExchangeName, HeaderProperty, LongUint, MessageCount, LongLongUint, DeliveryTag, FieldTable, ReservedShortString,
                                ReservedLongString, ReservedBit1, ReservedShortUint, ShortShortUint, ShortInt, LongInt, Float, Double, Decimal, LongLongInt, FieldArray,
                                TimeStamp, ShortShortInt)


# TODO test complex type Header FiledArray FiledTAble
# TODO add test for SASL type


class TestAmqpTypes:
    def test_amqp_len(self):
        length = 5
        obj = String('q' * length)
        assert len(obj) == length

    def test_amqp_add_and_equality(self):
        text1 = 'qwe'
        text2 = 'rty'
        assert String(text1) + String(text2) == String(text1 + text2)
        assert String(text1) + b'qwe' == String(text1 + 'qwe')
        iadd_obj = String(text1)
        iadd_obj += b'tratata'
        assert iadd_obj == String(text1 + 'tratata')

    def test_amqp_str_repr(self):
        assert str(String('qwe')) == "<class 'amqpsfw.amqp_types.String'> b'qwe'"
        assert repr(String('qwe')) == str(String('qwe'))

    # TODO test that decode return tail of bytes is rigth
    @pytest.mark.parametrize('types', [
        (String, 'qwqwr'), (String, b'qwqwr'), (ShortString, 'asfaf'), (ConsumerTag, 'zvzdbv'), (Path, 'asdggas'), (QueueName, 'sdgsdg'), (ExchangeName, 'asagaga'), (LongString, 'a' * 300),
        (LongString, bytes([1, 2, 3, 15])), (LongString, bytearray([1, 2, 3, 4])), (LongString, String('aqwrq')), (Char, 'c'),
        (Bool, True), (Bit1, [1]), (Bit2, [1, 1]), (Bit3, [1, 0, 1]), (Bit4, [1, 0, 1, 1]), (Bit5, [1, 1, 1, 0, 1]), (Bit6, [1, 0, 0, 0, 0, 1]), (Bit7, [1, 0, 0, 0, 0, 0, 1]),
        (Bit8, [1, 1, 1, 1, 1, 0, 0, 1]), (ShortUint, 124), (ShortShortUint, 12), (ShortInt, 12), (LongInt, 125), (ShortShortInt, 125),
        (HeaderProperty,
         dict(zip(HeaderProperty.properties_table, ['application/json', 'identity', dict(), 1, 2,
                                                    'asfa', 'asfasf', 'zxvz', 'gjfj', 1503404274,
                                                    'asfagf', 'dg', 'vncnv', 'afa']))
         ),
        (HeaderProperty, {'type': 'ttype'}),
        (LongUint, 12415454), (MessageCount, 12), (LongLongUint, 12414), (LongLongInt, 124), (DeliveryTag, 124124), (Float, 123.142), (Double, 121.4124),
        # TODO (Decimal, decimal.Decimal(12414.12414)),
        (TimeStamp, 1241), (FieldArray, ['qwe', True, 123, {'asd': 1.23}]),
        (FieldTable, {'host': 'localhost', 'qw': 2.1, 'asf': {'xc': [2, 200, 300, 'qwe', 'a' * 300, 33000, [], 70000, 2147483678, 4294967596]}}), (ReservedShortString, 'asfasf'),
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

    def test_field_negative(self):
        with pytest.raises(SfwException, match='Internal - Unknown type for filedtable type'):
            FieldTable({'qwe': (1, 2)})
        with pytest.raises(SfwException, match='Internal - Input type for FieldTable object is wrong'):
            FieldTable({1, 2})
        with pytest.raises(SfwException, match='Internal - Input type for FieldArray object is wrong'):
            FieldArray(2)

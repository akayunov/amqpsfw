import os
import sys

import pytest

sys.path = [os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib')] + sys.path

from amqpsfw.exceptions import SfwException
from amqpsfw.amqp_types import (AmqpType, String, Reserved, ShortString, ConsumerTag, Path, QueueName, LongString, Char, Octet, Bool, Bit1, Bit2, Bit3, Bit4, Bit5, Bit6, Bit7,
                                Bit8, ShortUint, ExchangeName, HeaderProperty, LongUint, MessageCount, LongLongUint, DeliveryTag, FieldTable, ReservedShortString,
                                ReservedLongString, ReservedBit1, ReservedShortUint)


class TestAmqpTypes:
    @pytest.mark.parametrize('types', [(String, 'qwqwr'), (ShortString, 'asfaf'), (ConsumerTag, 'zvzdbv'), (Path, 'asdggas'), (QueueName, 'sdgsdg'), (LongString, 'asgfagdsag'),
                                       (Char, 'c'), (Octet, 5), (Bool, 1), (Bit1, [1]), (Bit2, [1, 1]), (Bit3, [1, 0, 1]), (Bit4, [1, 0, 1, 1]),
                                       (Bit5, [1, 1, 1, 0, 1]), (Bit6, [1, 0, 0, 0, 0, 1]), (Bit7, [1, 0, 0, 0, 0, 0, 1]), (Bit8, [1, 1, 1, 1, 1, 0, 0, 1]), (ShortUint, 124),
                                       (ExchangeName, 'asfagsf'),
                                       (HeaderProperty, dict(zip(HeaderProperty.properties_table, ['application/json', 'identity', dict(), 1, 2,
                                                                                                   'asfa', 'asfasf', 'zxvz', 'gjfj', 1503404274,
                                                                                                   'asfagf', 'dg', 'vncnv', 'afa']))
                                        ), (LongUint, 12415454), (MessageCount, 12), (LongLongUint, 12414), (DeliveryTag, 124124),
                                       (FieldTable, {'host': ['S', 'localhost']}),
                                       (ReservedShortString, 'asfasf'),
                                       (ReservedLongString, 'asfsags'), (ReservedBit1, [1]), (ReservedShortUint, 1241)])
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

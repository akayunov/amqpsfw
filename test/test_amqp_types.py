import os
import sys
import unittest

import pytest

sys.path = [os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib')] + sys.path

from amqpsfw import amqp_types
from amqpsfw.exceptions import SfwException


class TestAmqpTypes:
    def test_octet(self):
        value = 15
        octet = amqp_types.Octet(value)
        assert (octet, b'') == amqp_types.Octet.decode(octet.encoded)



    def test_string(self):
        value = 'qwe'
        strint = amqp_types.String(value)
        assert strint.decoded_value == value
        assert strint.encoded == value.encode('utf8')
        with pytest.raises(SfwException):
            value = 1
            amqp_types.String(value)

    # def test_needsfiles(self, tmpdir):
    #     # print (tmpdir)
    #     assert 0
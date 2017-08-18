import sys
import unittest

import pytest

sys.path = ['/home/akayunov/sfw/sfw/lib'] + sys.path

from amqpsfw import amqp_types
from amqpsfw.exceptions import SfwException

class AmqpTypesTests(unittest.TestCase):
    def test_octet(self):
        value = 1
        octet = amqp_types.Octet(value)
        self.assertEqual(octet.decoded_value, 1)
        self.assertEqual(octet.encoded, bytes([1]))
        self.assertEqual(amqp_types.Octet.decode(octet.encoded), (amqp_types.Octet(1), b''))
        with pytest.raises(SfwException):
            value = '10000'
            amqp_types.Octet(value)
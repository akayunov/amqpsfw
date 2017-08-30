import os.path
import sys

sys.path = [os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib')] + sys.path
from amqpsfw.sasl_spec import Plain


class TestSaslSpec:
    def test_sals_plain(self):
        sals_string = Plain(['user', 'password'])
        assert (sals_string, b'') == Plain.decode(sals_string.encoded)

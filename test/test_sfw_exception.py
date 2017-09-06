import os.path
import sys

sys.path = [os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib')] + sys.path
from amqpsfw.exceptions import SfwException


class TestSwfException:
    def test_swf_exception(self):
        exc = SfwException('Internal', 'Error happend')
        assert exc.code == 'Internal'
        assert exc.msg == 'Error happend'
        assert str(exc) == 'Internal - Error happend'

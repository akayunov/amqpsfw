from amqpsfw.exceptions import SfwException


class TestSwfException:
    def test_swf_exception(self):
        exc = SfwException('Internal', 'Error happend')
        assert exc.code == 'Internal'
        assert exc.msg == 'Error happend'
        assert str(exc) == 'Internal - Error happend'
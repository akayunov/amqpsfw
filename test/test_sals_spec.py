from amqpsfw.sasl_spec import Plain


class TestSaslSpec:
    def test_sals_plain(self):
        sals_string = Plain(['user', 'password'])
        assert (sals_string, b'') == Plain.decode(sals_string.encoded)

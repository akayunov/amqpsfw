import os
import sys

import pytest

sys.path = [os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib')] + sys.path

from amqpsfw.exceptions import SfwException
from amqpsfw import amqp_spec, sasl_spec


class TestAmqpSpec:
    def test_len(self):
        tune = amqp_spec.Connection.Tune(channel_max=12, frame_max=13, heartbeat_interval=60, channel_number=0)
        print(tune.encoded)
        assert len(tune) == 1 + 2 + 4 + 2 + 2 + 2 + 4 + 2 + 1

    def test_protocol_header(self):
        p_header = amqp_spec.ProtocolHeader('A', 'M', 'Q', 'P', 0, 0, 9, 1)
        assert amqp_spec.decode_frame(p_header.encoded) == (len(p_header) - 8, p_header, b'')

    def test_heartbeat(self):
        heartbeat = amqp_spec.Heartbeat()
        assert amqp_spec.decode_frame(heartbeat.encoded) == (len(heartbeat) - 8, heartbeat, b'')

    def test_content_header(self):
        c_header = amqp_spec.Header(class_id=40, weight=15, body_size=1536136,
                                    header_properties={'content­encoding': 'identity', 'user­id': 'tratata', 'delivery­mode': 111}, channel_number=14)
        assert amqp_spec.decode_frame(c_header.encoded) == (len(c_header) - 8, c_header, b'')

    def test_content(self):
        content = amqp_spec.Content('sdgsgagasgd')
        assert amqp_spec.decode_frame(content.encoded) == (len(content) - 8, content, b'')
        content = amqp_spec.Content(b'sdgsgagasgd')
        assert amqp_spec.decode_frame(content.encoded) == (len(content) - 8, content, b'')

    def test_connection_start(self):
        s = amqp_spec.Connection.Start(version_major=3, version_minor=2, server_properties={'tratata': 121}, mechanisms='PLAIN', locale='en_US', channel_number=123)
        assert amqp_spec.decode_frame(s.encoded) == (len(s) - 8, s, b'')

    def test_connection_startok(self):
        start_ok = amqp_spec.Connection.StartOk(peer_properties={'tratata': 121}, mechanism='PLAIN', credential=['user', 'password'], locale='en_US',  channel_number=123)
        assert  amqp_spec.decode_frame(start_ok.encoded) == (len(start_ok) - 8, start_ok, b'')

    def test_connection_secure(self):
        secure = amqp_spec.Connection.Secure(challenge='tratatata', channel_number=124)
        assert amqp_spec.decode_frame(secure.encoded) == (len(secure) - 8, secure, b'')
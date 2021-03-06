import sys
import operator
from collections import OrderedDict
from amqpsfw import sasl_spec
from amqpsfw.amqp_types import (
    AmqpType, ShortUint, LongUint, FieldTable, ShortString, LongString, Char, Path, String,
    LongLongUint, ExchangeName, QueueName, MessageCount, HeaderProperty, ConsumerTag, DeliveryTag,
    Bit5, Bit1, Bit2, Bit4, Reserved, ReservedShortString, ReservedBit1, ReservedShortUint, ReservedLongString, ShortShortUint
)
from amqpsfw.exceptions import SfwException

THIS_MODULE = sys.modules[__name__]


class Frame:
    frame_end = ShortShortUint(206)
    frame_type = None
    type_structure = [ShortShortUint, ShortUint, LongUint]
    frame_params = []
    expected_response_frames = None

    def __init__(self, channel_number=0, **kwargs):
        self.payload = AmqpType('')
        self.set_payload(kwargs)
        self.channel_number = channel_number
        if type(channel_number) != int or not (65535 > channel_number >= 0):
            raise SfwException('Internal', 'Wrong channel number, must be int in [0 .. 65535]')
        if not self.frame_params:
            # init type
            self.set_frame_params(kwargs)
            self.set_expected_response_frames()

    def set_payload(self, kwargs):
        for arg_name, arg_value, arg_type in zip(kwargs.keys(), kwargs.values(), self.type_structure):
            if issubclass(arg_type, Bit1):
                arg_value = list(arg_value.values())
            try:
                self.payload += arg_type(arg_value)
            except Exception:
                raise SfwException('Internal', 'Wrong "' + arg_name + '" value for ' + str(type(self).__name__) + ' object/method')

    @property
    def encoded(self):
        return (ShortShortUint(self.frame_type) + ShortUint(self.channel_number) + LongUint(len(self.payload)) + self.payload + self.frame_end).encoded

    def set_expected_response_frames(self):
        if type(self).expected_response_frames:
            type(self).expected_response_frames = [operator.attrgetter(i)(THIS_MODULE) for i in type(self).expected_response_frames[:]]

    def set_frame_params(self, kwargs):
        type(self).frame_params = ['channel_number']
        for key in kwargs:
            if key.startswith('reserved') or key in ['method_method_id', 'method_class_id']:
                continue
            if type(kwargs[key]) is not OrderedDict:
                type(self).frame_params.append(key)
            else:
                type(self).frame_params.extend([i for i in kwargs[key].keys() if not i.startswith('reserved')])

    def __str__(self):
        return str(type(self)) + ' ' + ', '.join([str(k) + '=' + str(getattr(self, k)) for k in self.frame_params])

    def __eq__(self, other):
        if hasattr(other, 'encoded'):
            return self.encoded == other.encoded
        else:
            return False

    def __len__(self):
        return len(self.payload) + 8


class ProtocolHeader(Frame):
    frame_end = AmqpType('')
    type_structure = [Char, Char, Char, Char, ShortShortUint, ShortShortUint, ShortShortUint, ShortShortUint]
    expected_response_frames = ['Connection.Start']

    def __init__(self, c1, c2, c3, c4, v1, v2, v3, v4):
        super().__init__(channel_number=0, c1=c1, c2=c2, c3=c3, c4=c4, v1=v1, v2=v2, v3=v3, v4=v4)
        self.c1 = c1
        self.c2 = c2
        self.c3 = c3
        self.c4 = c4
        self.v1 = v1
        self.v2 = v2
        self.v3 = v3
        self.v4 = v4

    @property
    def encoded(self):
        return self.payload.encoded

    def __len__(self):
        return 8


class Method(Frame):
    frame_type = 1
    class_id = None
    method_id = None
    type_structure = [ShortUint, ShortUint]

    def __init__(self, **kwargs):
        self.type_structure = Method.type_structure + self.type_structure
        self.method_class_id = self.class_id
        self.method_method_id = self.method_id
        super().__init__(method_class_id=self.class_id, method_method_id=self.method_id, **kwargs)


class Header(Frame):
    frame_type = 2
    type_structure = [ShortUint, ShortUint, LongLongUint, HeaderProperty]

    def __init__(self, class_id, weight=0, body_size=0, header_properties=None, channel_number=0):
        super().__init__(class_id=class_id, weight=weight, body_size=body_size, header_properties=header_properties, channel_number=channel_number)
        self.class_id = class_id
        self.weight = weight
        self.body_size = body_size
        self.header_properties = header_properties


class Content(Frame):
    frame_type = 3
    type_structure = [String]

    def __init__(self, content='', channel_number=0):
        super().__init__(content=content, channel_number=channel_number)
        self.content = content


class Heartbeat(Frame):
    frame_type = 8
    type_structure = []


class Connection:
    class Start(Method):
        type_structure = [ShortShortUint, ShortShortUint, FieldTable, LongString, LongString]
        class_id = 10
        method_id = 10
        expected_response_frames = ['Connection.StartOk']

        def __init__(self, version_major, version_minor, server_properties, mechanisms, locale='en_US', channel_number=0):
            super().__init__(version_major=version_major, version_minor=version_minor, server_properties=server_properties, mechanisms=mechanisms,
                             locale=locale, channel_number=channel_number)
            self.version_major = version_major
            self.version_minor = version_minor
            self.server_properties = server_properties
            self.mechanisms = mechanisms
            self.locale = locale

    class StartOk(Method):
        type_structure = [FieldTable, ShortString, sasl_spec.Plain, ShortString]
        class_id = 10
        method_id = 11
        expected_response_frames = ['Connection.Secure', 'Connection.Tune']

        def __init__(self, client_properties, mechanisms, credential, locale='en_US', channel_number=0):
            super().__init__(client_properties=client_properties, mechanisms=mechanisms, credential=credential, locale=locale, channel_number=channel_number)
            self.client_properties = client_properties
            self.mechanisms = mechanisms
            self.credential = credential
            self.locale = locale

    class Secure(Method):
        type_structure = [LongString]
        class_id = 10
        method_id = 20
        expected_response_frames = ['Connection.SecureOk']

        def __init__(self, challenge, channel_number=0):
            super().__init__(challenge=challenge, channel_number=channel_number)
            self.challenge = challenge

    class SecureOk(Method):
        type_structure = [LongString]
        class_id = 10
        method_id = 21
        expected_response_frames = ['Connection.Tune']

        def __init__(self, response, channel_number=0):
            super().__init__(response=response, channel_number=channel_number)
            self.response = response

    class Tune(Method):
        type_structure = [ShortUint, LongUint, ShortUint]
        class_id = 10
        method_id = 30
        expected_response_frames = ['Connection.TuneOk']

        def __init__(self, channel_max=0, frame_max=131072, heartbeat_interval=60, channel_number=0):
            super().__init__(channel_max=channel_max, frame_max=frame_max, heartbeat_interval=heartbeat_interval, channel_number=channel_number)
            self.channel_max = channel_max
            self.frame_max = frame_max
            self.heartbeat_interval = heartbeat_interval

    class TuneOk(Method):
        type_structure = [ShortUint, LongUint, ShortUint]
        class_id = 10
        method_id = 31

        def __init__(self, channel_max=0, frame_max=131072, heartbeat_interval=60, channel_number=0):
            super().__init__(channel_max=channel_max, frame_max=frame_max, heartbeat_interval=heartbeat_interval, channel_number=channel_number)
            self.channel_max = channel_max
            self.frame_max = frame_max
            self.heartbeat_interval = heartbeat_interval

    class Open(Method):
        type_structure = [Path, ReservedShortString, ReservedBit1]
        class_id = 10
        method_id = 40
        expected_response_frames = ['Connection.OpenOk']

        def __init__(self, virtual_host='/', channel_number=0):
            super().__init__(virtual_host=virtual_host, reserved='', bit1=OrderedDict(reserved1=0), channel_number=channel_number)
            self.virtual_host = virtual_host

    class OpenOk(Method):
        type_structure = [ReservedShortString]
        class_id = 10
        method_id = 41
        expected_response_frames = ['Channel.Open']

        def __init__(self, channel_number=0):
            super().__init__(reserved='', channel_number=channel_number)

    class Close(Method):
        type_structure = [ShortUint, ShortString, ShortUint, ShortUint]
        class_id = 10
        method_id = 50
        expected_response_frames = ['Connection.CloseOk']

        def __init__(self, reply_code=0, reply_text='', class_id=10, method_id=50, channel_number=0):
            super().__init__(reply_code=reply_code, reply_text=reply_text, class_id=class_id, method_id=method_id, channel_number=channel_number)
            self.reply_code = reply_code
            self.reply_text = reply_text
            self.class_id = class_id
            self.method_id = method_id

    class CloseOk(Method):
        type_structure = []
        class_id = 10
        method_id = 51


class Channel:
    class Open(Method):
        type_structure = [ReservedShortString]
        class_id = 20
        method_id = 10
        expected_response_frames = ['Channel.OpenOk']

        def __init__(self, channel_number=0):
            super().__init__(reserved='', channel_number=channel_number)
            self.channel_number = channel_number

    class OpenOk(Method):
        type_structure = [ReservedLongString]
        class_id = 20
        method_id = 11
        expected_response_frames = ['Frame']

        def __init__(self, channel_number=0):
            super().__init__(reserved='', channel_number=channel_number)

    class Flow(Method):
        type_structure = [ReservedBit1]
        class_id = 20
        method_id = 20
        expected_response_frames = ['Channel.FlowOk']

        def __init__(self, active=1, channel_number=0):
            super().__init__(bit1=OrderedDict(active=active), channel_number=channel_number)
            self.active = active

    class FlowOk(Method):
        type_structure = [Bit1]
        class_id = 20
        method_id = 21
        expected_response_frames = ['Frame']

        def __init__(self, active=1, channel_number=0):
            super().__init__(bit1=OrderedDict(active=active), channel_number=channel_number)
            self.active = active

    class Close(Method):
        type_structure = [ShortUint, ShortString, ShortUint, ShortUint]
        class_id = 20
        method_id = 40
        expected_response_frames = ['Channel.CloseOk']

        def __init__(self, reply_code=0, reply_text='', class_id=20, method_id=40, channel_number=0):
            super().__init__(reply_code=reply_code, reply_text=reply_text, class_id=class_id, method_id=method_id, channel_number=channel_number)
            self.reply_code = reply_code
            self.reply_text = reply_text
            self.class_id = class_id
            self.method_id = method_id

    class CloseOk(Method):
        type_structure = []
        class_id = 20
        method_id = 41


class Exchange:
    class Declare(Method):
        type_structure = [ReservedShortUint, ExchangeName, ShortString, Bit5, FieldTable]
        class_id = 40
        method_id = 10
        expected_response_frames = ['Exchange.DeclareOk']

        def __init__(self, exchange_name, exchange_type='topic', do_not_create=0, durable=1, auto_deleted=0, internal=0, no_wait=0, properties=None,
                     channel_number=0):
            super().__init__(reserved=0, exchange_name=exchange_name, exchange_type=exchange_type,
                             bits5=OrderedDict(do_not_create=do_not_create, durable=durable, auto_deleted=auto_deleted, internal=internal, no_wait=no_wait), properties=properties,
                             channel_number=channel_number)
            self.exchange_name = exchange_name
            self.exchange_type = exchange_type
            self.do_not_create = do_not_create
            self.durable = durable
            self.auto_deleted = auto_deleted
            self.internal = internal
            self.no_wait = no_wait
            self.properties = properties
            self.channel_number = channel_number

    class DeclareOk(Method):
        type_structure = []
        class_id = 40
        method_id = 11
        expected_response_frames = ['Frame']

    class Delete(Method):
        type_structure = [ReservedShortUint, ExchangeName, Bit2]
        class_id = 40
        method_id = 20
        expected_response_frames = ['Exchange.DeleteOk']

        def __init__(self, exchange_name, delete_if_unused=0, no_wait=0, channel_number=0):
            super().__init__(reversed=0, exchange_name=exchange_name, bit2=OrderedDict(delete_if_unused=delete_if_unused, no_wait=no_wait), channel_number=channel_number)
            self.exchange_name = exchange_name
            self.delete_if_unused = delete_if_unused
            self.no_wait = no_wait

    class DeleteOk(Method):
        type_structure = []
        class_id = 40
        method_id = 21
        expected_response_frames = ['Frame']


class Queue:
    class Declare(Method):
        type_structure = [ReservedShortUint, QueueName, Bit5, FieldTable]
        class_id = 50
        method_id = 10
        expected_response_frames = ['Queue.DeclareOk']

        def __init__(self, queue_name, passive=0, durable=1, exclusive=0, auto_deleted=0, no_wait=0, properties=None, channel_number=0):
            super().__init__(reserved=0, queue_name=queue_name, bit5=OrderedDict(passive=passive, durable=durable, exclusive=exclusive, auto_deleted=auto_deleted, no_wait=no_wait),
                             properties=properties, channel_number=channel_number)
            self.queue_name = queue_name
            self.passive = passive
            self.durable = durable
            self.exclusive = exclusive
            self.auto_deleted = auto_deleted
            self.no_wait = no_wait
            self.properties = properties

    class DeclareOk(Method):
        type_structure = [QueueName, MessageCount, LongUint]
        class_id = 50
        method_id = 11
        expected_response_frames = ['Frame']

        def __init__(self, queue_name, message_count=0, consumer_count=0, channel_number=0):
            super().__init__(queue_name=queue_name, message_count=message_count, consumer_count=consumer_count, channel_number=channel_number)
            self.queue_name = queue_name
            self.message_count = message_count
            self.consumer_count = consumer_count

    class Bind(Method):
        type_structure = [ReservedShortUint, QueueName, ExchangeName, ShortString, Bit1, FieldTable]
        class_id = 50
        method_id = 20
        expected_response_frames = ['Queue.BindOk']

        def __init__(self, queue_name, exchange_name, routing_key, no_wait=0, properties=None, channel_number=0):
            super().__init__(reserved=0, queue_name=queue_name, exchange_name=exchange_name, routing_key=routing_key, bit1=OrderedDict(no_wait=no_wait), properties=properties,
                             channel_number=channel_number)
            self.queue_name = queue_name
            self.exchange_name = exchange_name
            self.routing_key = routing_key
            self.no_wait = no_wait
            self.properties = properties

    class BindOk(Method):
        type_structure = []
        class_id = 50
        method_id = 21
        expected_response_frames = ['Frame']


class Basic:
    class Publish(Method):
        type_structure = [ReservedShortUint, ExchangeName, ShortString, Bit2]
        class_id = 60
        method_id = 40

        def __init__(self, exchange_name, routing_key, mandatory=0, immediate=0, channel_number=0):
            super().__init__(reserved=0, exchange_name=exchange_name, routing_key=routing_key,
                             bit2=OrderedDict(mandatory=mandatory, immediate=immediate), channel_number=channel_number)
            self.exchange_name = exchange_name
            self.routing_key = routing_key
            self.mandatory = mandatory
            self.immediate = immediate

    class Consume(Method):
        type_structure = [ReservedShortUint, QueueName, ConsumerTag, Bit4, FieldTable]
        class_id = 60
        method_id = 20
        expected_response_frames = ['Basic.ConsumeOk']

        def __init__(self, queue_name, consumer_tag='', non_local=1, no_ack=0, exclusize=0, no_wait=0, properties=None, channel_number=0):
            super().__init__(reserved=0, queue_name=queue_name, consumer_tag=consumer_tag,
                             bit4=OrderedDict(non_local=non_local, no_ack=no_ack, exclusize=exclusize, no_wait=no_wait),
                             properties=properties, channel_number=channel_number)
            self.queue_name = queue_name
            self.consumer_tag = consumer_tag
            self.non_local = non_local
            self.no_ack = no_ack
            self.exclusize = exclusize
            self.no_wait = no_wait
            self.properties = properties

    class ConsumeOk(Method):
        type_structure = [ConsumerTag]
        class_id = 60
        method_id = 21
        expected_response_frames = ['Basic.Deliver']

        def __init__(self, consumer_tag='', channel_number=0):
            super().__init__(consumer_tag=consumer_tag, channel_number=channel_number)
            self.consumer_tag = consumer_tag

    class Deliver(Method):
        type_structure = [ConsumerTag, DeliveryTag, Bit1, ExchangeName, ShortString]
        class_id = 60
        method_id = 60
        expected_response_frames = ['Basic.Ack']

        def __init__(self, consumer_tag, delivery_tag, redelivered, exchange_name, routing_key, channel_number=0):
            super().__init__(consumer_tag=consumer_tag, delivery_tag=delivery_tag, bit1=OrderedDict(redelivered=redelivered), exchange_name=exchange_name, routing_key=routing_key,
                             channel_number=channel_number)
            self.consumer_tag = consumer_tag
            self.delivery_tag = delivery_tag
            self.redelivered = redelivered
            self.exchange_name = exchange_name
            self.routing_key = routing_key

    class Ack(Method):
        type_structure = [DeliveryTag, Bit1]
        class_id = 60
        method_id = 80

        def __init__(self, delivery_tag, multiple=0, channel_number=0):
            super().__init__(delivery_tag=delivery_tag, bit1=OrderedDict(multiple=multiple), channel_number=channel_number)
            self.delivery_tag = delivery_tag
            self.multiple = multiple


class Tx:
    class Select(Method):
        type_structure = []
        class_id = 90
        method_id = 10
        expected_response_frames = ['Tx.SelectOk']

    class SelectOK(Method):
        type_structure = []
        class_id = 90
        method_id = 11
        expected_response_frames = ['Frame']

    class Commit(Method):
        type_structure = []
        class_id = 90
        method_id = 20
        expected_response_frames = ['Tx.CommitOk']

    class CommitOK(Method):
        type_structure = []
        class_id = 90
        method_id = 21
        expected_response_frames = ['Frame']

    class Rollback(Method):
        type_structure = []
        class_id = 90
        method_id = 30
        expected_response_frames = ['Tx.RollbackOk']

    class RollbackOK(Method):
        type_structure = []
        class_id = 90
        method_id = 31
        expected_response_frames = ['Frame']


FRAME_TYPES = {
    1: {
        10: {
            10: Connection.Start,
            11: Connection.StartOk,
            20: Connection.Secure,
            21: Connection.SecureOk,
            30: Connection.Tune,
            31: Connection.TuneOk,
            40: Connection.Open,
            41: Connection.OpenOk,
            50: Connection.Close,
            51: Connection.CloseOk
        },
        20: {
            10: Channel.Open,
            11: Channel.OpenOk,
            20: Channel.Flow,
            21: Channel.FlowOk,
            40: Channel.Close,
            41: Channel.CloseOk
        },
        40: {
            10: Exchange.Declare,
            11: Exchange.DeclareOk,
            20: Exchange.Delete,
            21: Exchange.DeleteOk
        },
        50: {
            10: Queue.Declare,
            11: Queue.DeclareOk,
            20: Queue.Bind,
            21: Queue.BindOk
        },
        60: {
            40: Basic.Publish,
            20: Basic.Consume,
            21: Basic.ConsumeOk,
            60: Basic.Deliver
        },
    },
    2: Header,
    3: Content,
    8: Heartbeat
}


def decode_frame(frame_bytes):
    if len(frame_bytes) < 8:
        return 0, None, frame_bytes
    if frame_bytes.startswith(b'AMQP') and len(frame_bytes) >= 8:
        result = []
        for i in ProtocolHeader.type_structure:
            payload_part, frame_bytes = i.decode(frame_bytes)
            result.append(payload_part.decoded_value)
        return 0, ProtocolHeader(*result), frame_bytes[8:]

    (frame_type, _), (frame_channel, _), (payload_size, _) = ShortShortUint.decode(bytes([frame_bytes[0]])), ShortUint.decode(frame_bytes[1:3]), LongUint.decode(frame_bytes[3:7])
    if payload_size.decoded_value > len(frame_bytes[7:]) - 1:
        # data is non efficient
        return payload_size.decoded_value, None, frame_bytes
    method_bytes, frame_end = frame_bytes[7:payload_size.decoded_value + 7], frame_bytes[payload_size.decoded_value + 7]
    if frame_end != frame_end:
        raise SfwException('Internal', 'Wrong frame end')
    if payload_size.decoded_value != len(method_bytes):
        raise SfwException('Internal', 'Wrong frame size')
    if frame_type == ShortShortUint(8):
        return 0, FRAME_TYPES[frame_type.decoded_value](), frame_bytes[payload_size.decoded_value + 7 + 1:]
    elif frame_type in [ShortShortUint(2), ShortShortUint(3)]:
        result = []
        for i in FRAME_TYPES[frame_type.decoded_value].type_structure:
            payload_part, method_bytes = i.decode(method_bytes)
            result.append(payload_part.decoded_value)
        return payload_size.decoded_value, FRAME_TYPES[frame_type.decoded_value](*result, channel_number=frame_channel.decoded_value), frame_bytes[
                                                                                                                                       payload_size.decoded_value + 7 + 1:]
    else:
        result = []
        (class_id, _), (method_id, _), method_payload = ShortUint.decode(method_bytes[0:2]), ShortUint.decode(method_bytes[2:4]), method_bytes[4: len(method_bytes)]
        for i in FRAME_TYPES[frame_type.decoded_value][class_id.decoded_value][method_id.decoded_value].type_structure:
            payload_part, method_payload = i.decode(method_payload)
            if issubclass(i, Reserved):
                continue
            elif issubclass(i, Bit1):
                result.extend(payload_part.decoded_value)
            else:
                result.append(payload_part.decoded_value)
        return (
            payload_size.decoded_value,
            FRAME_TYPES[frame_type.decoded_value][class_id.decoded_value][method_id.decoded_value](*result, channel_number=frame_channel.decoded_value),
            frame_bytes[payload_size.decoded_value + 7 + 1:]
        )

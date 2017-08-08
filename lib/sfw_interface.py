import amqp_types
import amqp_spec


def protocol_header(version):
    r = amqp_spec.ProtocolHeader([amqp_types.Octet(version[0]), amqp_types.Octet(version[1]),amqp_types.Octet(version[2]), amqp_types.Octet(version[3])])
    print(r)
    return r


def start_ok(host, sals_mechanizm, user, passwd, locale):
    r = amqp_spec.Connection.StartOk(arguments=[{'host': ['S', host]}, sals_mechanizm, (amqp_types.Octet(0) + user.encode('utf8') + amqp_types.Octet(0) + passwd.encode('utf8')), locale])
    print(r)
    return r


def tune_ok(channel_max, frame_max, heartbeat):
    r = amqp_spec.Connection.TuneOk(arguments=[channel_max, frame_max, heartbeat])
    print(r)
    return r


def conection_open(virtual_host):
    r = amqp_spec.Connection.Open(arguments=[virtual_host, '', 0])
    print(r)
    return r


def hearbeat():
    r = amqp_spec.Heartbeat()
    print(r)
    return r


def channel_open(channel_number):
    r = amqp_spec.Channel.Open(channel_number, [''])
    print(r)
    return r


def flow(channel_number, flow_start):
    r = amqp_spec.Channel.Flow(channel_number, [flow_start])
    print(r)
    return r


def exchange_declare(channel_number, exchange_name, exchange_type, exchange_passive, exchange_durable, exchange_auto_deleted, exchange_internal, exchange_no_wait, exchange_table, reserved_short=1):
            # >> if bit go together when pack it in one octet
            # 11000000 -> reverse order -> 00000011 -> 3
            # exchange_passive,exchange_durable,exchange_auto_deleted,exchange_internal,exchange_no_wait
    bits = exchange_passive * 2 ** 0 + exchange_durable * 2 ** 1 + exchange_auto_deleted * 2 ** 2 + exchange_internal * 2 ** 3 + exchange_no_wait * 2 ** 4
            # <<< if bit go together when pack it in one octet
    r = amqp_spec.Exchange.Declare(channel_number, [reserved_short, exchange_name, exchange_type, bits, exchange_table])
    print(r)
    return r


def queue_declare(channel_number, queue_name, queue_passive, queue_durable, queue_exclusive, queue_auto_deleted, queue_no_wait, queue_table):
    # >> if bit go together when pack it in one octet
    # 01000000 -> reverse order -> 00000010 -> 2
    # queue_passive,queue_durable,queue_exclusive,queue_auto_deleted,queue_no_wait
    bits = queue_passive * 2 ** 0 + queue_durable * 2 ** 1 + queue_exclusive * 2 ** 2 + queue_auto_deleted * 2 ** 3 + queue_no_wait * 2 ** 4
    # <<< if bit go together when pack it in one octet
    r = amqp_spec.Queue.Declare(channel_number, [1, queue_name, bits, queue_table])
    print(r)
    return r


def queue_bind(channel_number, queue_name, exchange_name, routing_key, no_wait, arguments):
    # >> if bit go together when pack it in one octet
    # 11000000 -> reverse order -> 00000011 -> 3
    # no_wait
    bits = no_wait
    # <<< if bit go together when pack it in one octet
    r = amqp_spec.Queue.Bind(channel_number, [1, queue_name, exchange_name, routing_key, bits, arguments])
    print(r)
    return r


def publish(channel_number,exchange_name, routing_key, mandatory, immediate, content_string, property):
    # >> if bit go together when pack it in one octet
    # 11000000 -> reverse order -> 00000011 -> 3
    # no_wait

    bits = mandatory * 2 ** 0 + immediate * 2 ** 1
    # <<< if bit go together when pack it in one octet
    # content header
    content_string = content_string.encode('utf8')

    class_id = amqp_spec.Basic.Publish.class_id
    weight = amqp_types.ShortUint(0)
    body_size = amqp_types.LongLongUint(len(content_string))
    # property by order first property - highest bit 1000000000000000 - only first property
    properties_table = ['content-type']
    property_flag = 0
    property_values = []
    for k in property:
        property_flag += 2 ** (15 - properties_table.index(k))
        property_values.append(amqp_types.ShortString(property[k]))
    property_flag = amqp_types.ShortUint(property_flag)
    r = [
        amqp_spec.Basic.Publish(channel_number, [1, exchange_name, routing_key, bits]),
        amqp_spec.Header(channel_number, [amqp_types.ShortUint(class_id), weight, body_size, property_flag, *property_values]),
        amqp_spec.Content(channel_number, [content_string])
    ]

    [print(i) for i in r]
    return r

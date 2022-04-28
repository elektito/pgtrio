from ipaddress import ip_address, ip_network
from datetime import datetime, date, time, timedelta, timezone
from pgtrio import _codecs


def test():
    test_values = {
        _codecs.Bool: [False, True],
        _codecs.ByteA: [b'', b'foobar'],
        _codecs.Int2: [-2**15, 2**15-1, 0, 1000, -998],
        _codecs.Int4: [-2**31, 2**31-1, 0, -998],
        _codecs.Int8: [-2**63, 2**63-1, 0, -998],
        _codecs.Text: ['', 'spam', 'eggs'],
        _codecs.Json: [{}, [], {'foo': 100.5}, [{}, [{}], {'a': [1]}]],
        _codecs.Jsonb: [{}, [], {'foo': 100.5}, [{}, [{}], {'a': [1]}]],
        _codecs.Float4: [1.125-38, 2.5+38, 0, -1.5],
        _codecs.Float8: [
            -1.7976931348623157e+308,
            1.7976931348623157e+308,
            1.7976931348623157e-308,
            0,
            1.5,
        ],
        _codecs.Inet: [ip_address('192.168.1.1'), ip_address('::3')],
        _codecs.Cidr: [
            ip_network('192.168.0.0/16'),
            ip_network('::/8')
        ],
        _codecs.Char: ['\x00', 'A'],
        _codecs.Date: [date(1982, 11, 28), date(1, 1, 2)],
        _codecs.Time: [time(0, 0, 1), time(18, 19, 44)],
        _codecs.DateTime: [datetime(2022, 4, 18, 1, 31, 43, 646120)],
        _codecs.DateTimeTz: [
            datetime(2022, 4, 18, 1, 31, 43, 646120,
                     tzinfo=timezone(timedelta(hours=2, minutes=30))),
        ],
        _codecs.Interval: [
            timedelta(hours=2, minutes=30),
            timedelta(days=41, hours=82, minutes=30),
            timedelta(days=41, hours=82, minutes=30, seconds=7),
            timedelta(days=41+100*365, hours=82, minutes=30, seconds=7),
            timedelta(days=41+300*365, hours=82, minutes=30, seconds=7),
            timedelta(days=100*365),
            timedelta(days=100*365, microseconds=2),
            timedelta(days=1*365),
            timedelta(days=-1*365),
            timedelta(days=-1*365, seconds=2),
            timedelta(days=2*365, seconds=-2),
            timedelta(days=3*365+1*30+1, hours=4, minutes=47, seconds=58),
            timedelta(days=9*365+1*30+1, hours=4, minutes=47, seconds=58),
            timedelta(days=9*365+9*30+1, hours=4, minutes=47, seconds=58),
            timedelta(days=9*365+9*30+1, hours=-5, minutes=-4, seconds=-2),
            timedelta(days=1, microseconds=2100),
            timedelta(days=21),
            timedelta(days=2107),
            timedelta(hours=-10),
            timedelta(hours=10),
        ],
        _codecs.TimeTz: [
            time(0, 0, 1, tzinfo=timezone(timedelta(hours=2))),
            time(18, 19, 44, tzinfo=timezone(timedelta(hours=2))),
        ],
    }

    for codec, codec_test_values in test_values.items():
        for value in codec_test_values:
            enc = codec.encode_text(value)
            v = codec.decode_text(enc)
            assert v == value, f'Text codec failure: {codec.__name__}'

            enc = codec.encode_binary(value)
            v = codec.decode_binary(enc)
            assert v == value, f'Binary codec failure: {codec.__name__}'

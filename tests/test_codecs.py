from ipaddress import ip_address, ip_network
from datetime import datetime, date, time, timedelta, timezone
from pgtrio import _codecs


def _test_codec(codec, test_values):
    for value in test_values:
        enc = codec.encode_text(value)
        v = codec.decode_text(enc)
        assert v == value, f'Text codec failure: {codec.__name__}'

        enc = codec.encode_binary(value)
        v = codec.decode_binary(enc)
        assert v == value, f'Binary codec failure: {codec.__name__}'


def test_bool():
    _test_codec(_codecs.Bool, [True, False])


def test_bytea():
    _test_codec(_codecs.ByteA, [b'', b'foobar'])


def test_int2():
    _test_codec(_codecs.Int2, [-2**15, 2**15-1, 0, 1000, -998])


def test_int4():
    _test_codec(_codecs.Int4, [-2**31, 2**31-1, 0, -998])


def test_int8():
    _test_codec(_codecs.Int8, [-2**63, 2**63-1, 0, -998])


def test_text():
    _test_codec(_codecs.Text, ['', 'spam', 'eggs'])


def test_json():
    _test_codec(
        _codecs.Json,
        [{}, [], {'foo': 100.5}, [{}, [{}], {'a': [1]}]])


def test_jsonb():
    _test_codec(
        _codecs.Jsonb,
        [{}, [], {'foo': 100.5}, [{}, [{}], {'a': [1]}]])


def test_float4():
    _test_codec(_codecs.Float4, [1.125-38, 2.5+38, 0, -1.5])


def test_float8():
    _test_codec(_codecs.Float8, [
        -1.7976931348623157e+308,
        1.7976931348623157e+308,
        1.7976931348623157e-308,
        0,
        1.5,
    ])


def test_inet():
    _test_codec(
        _codecs.Inet,
        [ip_address('192.168.1.1'), ip_address('::3')])


def test_cidr():
    _test_codec(_codecs.Cidr, [
        ip_network('192.168.0.0/16'),
        ip_network('::/8')
    ])


def test_char():
    _test_codec(_codecs.Char, ['\x00', 'A'])


def test_date():
    _test_codec(_codecs.Date, [date(1982, 11, 28), date(1, 1, 2)])


def test_time():
    _test_codec(_codecs.Time, [time(0, 0, 1), time(18, 19, 44)])


def test_datetime():
    _test_codec(
        _codecs.DateTime,
        [datetime(2022, 4, 18, 1, 31, 43, 646120)])


def test_datetimetz():
    _test_codec(_codecs.DateTimeTz, [
        datetime(2022, 4, 18, 1, 31, 43, 646120,
                 tzinfo=timezone(timedelta(hours=2, minutes=30))),
    ])


def test_interval():
    _test_codec(_codecs.Interval, [
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
    ])


def test_timetz():
    _test_codec(_codecs.TimeTz, [
        time(0, 0, 1, tzinfo=timezone(timedelta(hours=2))),
        time(18, 19, 44, tzinfo=timezone(timedelta(hours=2))),
    ])

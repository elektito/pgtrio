from ipaddress import IPv4Address, IPv6Address
from datetime import datetime, date, time, timedelta, timezone
from pytest import fixture, raises, mark
from utils import postgres_socket_file, conn
import pgtrio


async def test_null_in_query(conn):
    with raises(pgtrio.ProgrammingError):
        await conn.execute('foo \x00 bar')


async def test_empty_query(conn):
    results = await conn.execute('')
    assert results == []
    assert conn.rowcount == 0


async def test_float4(conn):
    await conn.execute('create table foobar (foo float4)')
    await conn.execute('insert into foobar (foo) values (4.25)')
    await conn.execute('insert into foobar (foo) values (18.5)')
    results = await conn.execute('select * from foobar')
    assert conn.rowcount == 2
    assert results == [(4.25,), (18.5,)]


async def test_float8(conn):
    await conn.execute('create table foobar (foo float8)')
    await conn.execute('insert into foobar (foo) values (4.25)')
    await conn.execute('insert into foobar (foo) values (18.5)')
    results = await conn.execute('select * from foobar')
    assert conn.rowcount == 2
    assert results == [(4.25,), (18.5,)]


async def test_int2(conn):
    await conn.execute('create table foobar (foo int2)')
    await conn.execute('insert into foobar (foo) values (1000)')
    await conn.execute('insert into foobar (foo) values (-32768)')
    await conn.execute('insert into foobar (foo) values (32767)')
    await conn.execute('insert into foobar (foo) values (0)')
    results = await conn.execute('select * from foobar')
    assert conn.rowcount == 4
    assert results == [(1000,), (-32768,), (32767,), (0,)]


async def test_int2_out_of_range(conn):
    await conn.execute('create table foobar (foo int2)')
    with raises(pgtrio.DatabaseError):
        await conn.execute('insert into foobar (foo) values (33000)')


async def test_int4(conn):
    await conn.execute('create table foobar (foo int4)')
    await conn.execute('insert into foobar (foo) values (1000)')
    await conn.execute('insert into foobar (foo) values (-2147483648)')
    await conn.execute('insert into foobar (foo) values (2147483647)')
    await conn.execute('insert into foobar (foo) values (0)')
    results = await conn.execute('select * from foobar')
    assert conn.rowcount == 4
    assert results == [(1000,), (-2147483648,), (2147483647,), (0,)]


async def test_int4_out_of_range(conn):
    await conn.execute('create table foobar (foo int4)')
    with raises(pgtrio.DatabaseError):
        await conn.execute(
            'insert into foobar (foo) values (2147483648)')


async def test_int8(conn):
    await conn.execute('create table foobar (foo int8)')
    await conn.execute('insert into foobar (foo) values (1000)')
    await conn.execute(
        'insert into foobar (foo) values (-9223372036854775808)')
    await conn.execute(
        'insert into foobar (foo) values (9223372036854775807)')
    await conn.execute('insert into foobar (foo) values (0)')
    results = await conn.execute('select * from foobar')
    assert conn.rowcount == 4
    assert results == [
        (1000,),
        (-9223372036854775808,),
        (9223372036854775807,),
        (0,)
    ]

async def test_int8_out_of_range(conn):
    await conn.execute('create table foobar (foo int8)')
    with raises(pgtrio.DatabaseError):
        await conn.execute(
            'insert into foobar (foo) values (9223372036854775808)')


async def test_bool(conn):
    await conn.execute('create table foobar (foo bool)')
    await conn.execute("insert into foobar (foo) values ('t')")
    await conn.execute("insert into foobar (foo) values ('f')")
    results = await conn.execute('select * from foobar')
    assert conn.rowcount == 2
    assert results == [(True,), (False,)]


async def test_bytea(conn):
    await conn.execute('create table foobar (foo bytea)')
    await conn.execute("insert into foobar (foo) values ('\\x000102')")
    results = await conn.execute('select * from foobar')
    assert conn.rowcount == 1
    assert results == [(b'\x00\x01\x02',),]


async def test_bytea(conn):
    await conn.execute('create table foobar (foo text)')
    await conn.execute("insert into foobar (foo) values ('fooobaar')")
    results = await conn.execute('select * from foobar')
    assert conn.rowcount == 1
    assert results == [('fooobaar',),]


async def test_json(conn):
    await conn.execute('create table foobar (foo json)')
    await conn.execute("insert into foobar (foo) values ('{}')")
    await conn.execute("insert into foobar (foo) values ('[]')")
    await conn.execute(
        "insert into foobar (foo) values "
        "('{\"foo\": [10], \"bar\": 1.5}')")
    results = await conn.execute('select * from foobar')
    assert conn.rowcount == 3
    assert results == [
        ({},),
        ([],),
        ({'foo': [10], 'bar': 1.5},),
    ]


async def test_jsonb(conn):
    await conn.execute('create table foobar (foo jsonb)')
    await conn.execute("insert into foobar (foo) values ('{}')")
    await conn.execute("insert into foobar (foo) values ('[]')")
    await conn.execute(
        "insert into foobar (foo) values "
        "('{\"foo\": [10], \"bar\": 1.5}')")
    results = await conn.execute('select * from foobar')
    assert conn.rowcount == 3
    assert results == [
        ({},),
        ([],),
        ({'foo': [10], 'bar': 1.5},),
    ]


async def test_inet(conn):
    await conn.execute('create table foobar (foo inet)')
    await conn.execute(
        "insert into foobar (foo) values ('192.168.1.100')")
    await conn.execute("insert into foobar (foo) values ('::2')")
    results = await conn.execute('select * from foobar')
    assert conn.rowcount == 2
    assert results == [
        (IPv4Address('192.168.1.100'),),
        (IPv6Address('::2'),),
    ]


async def test_char(conn):
    await conn.execute('create table foobar (foo char)')
    await conn.execute("insert into foobar (foo) values ('X')")
    results = await conn.execute('select * from foobar')
    assert conn.rowcount == 1
    assert results == [
        ('X',),
    ]


async def test_varchar(conn):
    await conn.execute('create table foobar (foo varchar)')
    await conn.execute("insert into foobar (foo) values ('abc')")
    await conn.execute("insert into foobar (foo) values ('xyzabc')")
    results = await conn.execute('select * from foobar')
    assert conn.rowcount == 2
    assert results == [
        ('abc',),
        ('xyzabc',),
    ]


async def test_date(conn):
    await conn.execute('create table foobar (foo date)')
    await conn.execute("insert into foobar (foo) values ('2022-10-19')")
    await conn.execute("insert into foobar (foo) values ('1954-01-02')")
    results = await conn.execute('select * from foobar')
    assert conn.rowcount == 2
    assert results == [
        (date(2022, 10, 19),),
        (date(1954, 1, 2),),
    ]


async def test_time(conn):
    await conn.execute('create table foobar (foo time)')
    await conn.execute("insert into foobar (foo) values ('00:00:01')")
    await conn.execute("insert into foobar (foo) values ('18:19:44')")
    results = await conn.execute('select * from foobar')
    assert conn.rowcount == 2
    assert results == [
        (time(0, 0, 1),),
        (time(18, 19, 44),),
    ]


async def test_timestamp(conn):
    await conn.execute('create table foobar (foo timestamp)')
    await conn.execute("insert into foobar (foo) values ('2022-04-18 01:31:43.64612')")
    results = await conn.execute('select * from foobar')
    assert conn.rowcount == 1
    assert results == [
        (datetime(2022, 4, 18, 1, 31, 43, 646120),),
    ]


async def test_timestamptz(conn):
    await conn.execute('create table foobar (foo timestamptz)')
    await conn.execute("insert into foobar (foo) values ('2022-04-18 01:31:43.64612+2:30')")
    results = await conn.execute('select * from foobar')
    assert conn.rowcount == 1
    assert results == [
        (datetime(2022, 4, 18, 1, 31, 43, 646120,
                  tzinfo=timezone(timedelta(hours=2, minutes=30))),),
    ]


async def test_interval(conn):
    await conn.execute('create table foobar (foo interval)')
    await conn.execute("insert into foobar (foo) values ('PT2H30M')")
    await conn.execute("insert into foobar (foo) values ('P41DT82H30M')")
    await conn.execute("insert into foobar (foo) values ('P41DT82H30M7S')")
    await conn.execute("insert into foobar (foo) values ('P100Y41DT82H30M7S')")
    await conn.execute("insert into foobar (foo) values ('P300Y41DT82H30M7S')")
    await conn.execute("insert into foobar (foo) values ('P100Y')")
    await conn.execute("insert into foobar (foo) values ('P100YT0.000002S')")
    await conn.execute("insert into foobar (foo) values ('P1Y')")
    await conn.execute("insert into foobar (foo) values ('P-1Y')")
    await conn.execute("insert into foobar (foo) values ('P-1YT2S')")
    await conn.execute("insert into foobar (foo) values ('P2YT-2S')")
    await conn.execute("insert into foobar (foo) values ('P3Y1M1DT4H47M58S')")
    await conn.execute("insert into foobar (foo) values ('P9Y1M1DT4H47M58S')")
    await conn.execute("insert into foobar (foo) values ('P9Y9M1DT4H47M58S')")
    await conn.execute("insert into foobar (foo) values ('P9Y9M1DT-5H-4M-2S')")
    await conn.execute("insert into foobar (foo) values ('P1DT0.0021S')")
    await conn.execute("insert into foobar (foo) values ('P21D')")
    await conn.execute("insert into foobar (foo) values ('P2107D')")
    await conn.execute("insert into foobar (foo) values ('PT-10H')")
    await conn.execute("insert into foobar (foo) values ('PT10H')")
    results = await conn.execute('select * from foobar')
    assert conn.rowcount == 20
    assert results == [
        (timedelta(hours=2, minutes=30),),
        (timedelta(days=41, hours=82, minutes=30),),
        (timedelta(days=41, hours=82, minutes=30, seconds=7),),
        (timedelta(days=41+100*365, hours=82, minutes=30, seconds=7),),
        (timedelta(days=41+300*365, hours=82, minutes=30, seconds=7),),
        (timedelta(days=100*365),),
        (timedelta(days=100*365, microseconds=2),),
        (timedelta(days=1*365),),
        (timedelta(days=-1*365),),
        (timedelta(days=-1*365, seconds=2),),
        (timedelta(days=2*365, seconds=-2),),
        (timedelta(days=3*365+1*30+1, hours=4, minutes=47, seconds=58),),
        (timedelta(days=9*365+1*30+1, hours=4, minutes=47, seconds=58),),
        (timedelta(days=9*365+9*30+1, hours=4, minutes=47, seconds=58),),
        (timedelta(days=9*365+9*30+1, hours=-5, minutes=-4, seconds=-2),),
        (timedelta(days=1, microseconds=2100),),
        (timedelta(days=21),),
        (timedelta(days=2107),),
        (timedelta(hours=-10),),
        (timedelta(hours=10),),
    ]


async def test_timetz(conn):
    await conn.execute('create table foobar (foo timetz)')
    await conn.execute("insert into foobar (foo) values ('00:00:01+02')")
    await conn.execute("insert into foobar (foo) values ('18:19:44+02')")
    results = await conn.execute('select * from foobar')
    assert conn.rowcount == 2
    assert results == [
        (time(0, 0, 1, tzinfo=timezone(timedelta(hours=2))),),
        (time(18, 19, 44, tzinfo=timezone(timedelta(hours=2))),),
    ]

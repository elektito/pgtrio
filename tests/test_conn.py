from pytest import fixture, raises
from utils import postgres_socket_file, conn
import pgtrio


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

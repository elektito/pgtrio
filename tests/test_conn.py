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

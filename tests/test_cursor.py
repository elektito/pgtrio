from pytest import raises
from utils import postgres_socket_file, conn
from pgtrio import ProgrammingError


async def test_no_transaction(conn):
    await conn.execute('create table foobar (foo int)')

    with raises(ProgrammingError):
        cur = await conn.cursor('select foo from foobar order by foo')


async def test_simple(conn):
    await conn.execute('create table foobar (foo int)')
    for i in range(5):
        await conn.execute('insert into foobar (foo) values ($1)', i)

    async with conn.transaction():
        cur = await conn.cursor('select foo from foobar order by foo')

        result = await cur.fetch_row()
        assert result == (0,)

        result = await cur.fetch(2)
        assert result == [(1,), (2,)]

        result = await cur.fetch(2)
        assert result == [(3,), (4,)]

        result = await cur.fetch(2)
        assert result == []

        result = await cur.fetch_row()
        assert result is None

        # should have no effect
        await cur.forward(10)


async def test_iter(conn):
    await conn.execute('create table foobar (foo int)')
    for i in range(100):
        await conn.execute('insert into foobar (foo) values ($1)', i * 100)

    async with conn.transaction():
        cur = await conn.cursor('select * from foobar order by foo')
        await cur.forward(10)

        i = 10
        async for row in cur:
            assert row == (i * 100,)
            i += 1


async def test_prepared_stmt_no_transaction(conn):
    await conn.execute('create table foobar (foo int)')
    stmt = await conn.prepare('select * from foobar')
    with raises(ProgrammingError):
        cur = await stmt.cursor()


async def test_prepared_stmt(conn):
    stmt = await conn.prepare('select generate_series(0, 3)')
    async with conn.transaction():
        cur = await stmt.cursor()
        assert await cur.fetch_row() == (0,)
        assert await cur.fetch_row() == (1,)
        assert await cur.fetch_row() == (2,)
        assert await cur.fetch_row() == (3,)

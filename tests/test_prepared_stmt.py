import trio
from pytest import raises
from utils import postgres_socket_file, conn
from pgtrio._exceptions import InterfaceError, ProgrammingError


async def test_simple(conn):
    await conn.execute('create table foobar (foo int)')

    stmt = await conn.prepare('insert into foobar (foo) values (10)')
    await stmt.execute()

    results = await conn.execute('select * from foobar')
    assert results == [(10,)]


async def test_multipart(conn):
    await conn.execute('create table foobar (foo int)')
    await conn.execute('insert into foobar (foo) values (10)')
    await conn.execute('insert into foobar (foo) values (20)')
    await conn.execute('insert into foobar (foo) values (30)')

    stmt = await conn.prepare('select * from foobar')

    async with conn.transaction():
        results = await stmt.execute(limit=1)
        assert results == [(10,)]

        results = await stmt.exec_continue(limit=1)
        assert results == [(20,)]


async def test_forward(conn):
    await conn.execute('create table foobar (foo int)')
    await conn.execute('insert into foobar (foo) values (10)')
    await conn.execute('insert into foobar (foo) values (20)')
    await conn.execute('insert into foobar (foo) values (30)')

    stmt = await conn.prepare('select * from foobar')

    async with conn.transaction():
        results = await stmt.execute(limit=1)
        assert results == [(10,)]

        await stmt.forward(1)

        results = await stmt.exec_continue(limit=1)
        assert results == [(30,)]


async def test_non_matching_transactions(conn):
    await conn.execute('create table foobar (foo int)')
    await conn.execute('insert into foobar (foo) values (10)')
    await conn.execute('insert into foobar (foo) values (20)')
    await conn.execute('insert into foobar (foo) values (30)')

    stmt = await conn.prepare('select * from foobar')

    async with conn.transaction():
        await stmt.execute(limit=1)

    with raises(ProgrammingError):
        async with conn.transaction():
            await stmt.exec_continue(limit=1)


async def test_finished(conn):
    await conn.execute('create table foobar (foo int)')
    await conn.execute('insert into foobar (foo) values (10)')
    await conn.execute('insert into foobar (foo) values (20)')
    await conn.execute('insert into foobar (foo) values (30)')

    stmt = await conn.prepare('select * from foobar')
    assert not stmt.finished

    async with conn.transaction():
        await stmt.execute(limit=1)
        assert not stmt.finished

        await stmt.exec_continue(limit=1)
        assert not stmt.finished

        await stmt.exec_continue()
        assert stmt.finished

        await stmt.execute(limit=1)
        assert not stmt.finished


async def test_multipart_no_transaction1(conn):
    await conn.execute('create table foobar (foo int)')
    await conn.execute('insert into foobar (foo) values (10)')
    await conn.execute('insert into foobar (foo) values (20)')
    await conn.execute('insert into foobar (foo) values (30)')

    stmt = await conn.prepare('select * from foobar')

    # using limit outside a transaction block is not allowed; because
    # when you want to do exec_continue, postgres will complain that
    # "portal does not exist"
    with raises(InterfaceError):
        await stmt.execute(limit=1)


async def test_multipart_no_transaction2(conn):
    await conn.execute('create table foobar (foo int)')
    await conn.execute('insert into foobar (foo) values (10)')
    await conn.execute('insert into foobar (foo) values (20)')
    await conn.execute('insert into foobar (foo) values (30)')

    stmt = await conn.prepare('select * from foobar')

    async with conn.transaction():
        await stmt.execute(limit=1)

    with raises(InterfaceError):
        await stmt.exec_continue()


async def test_multiple(conn):
    await conn.execute('create table foobar (foo int)')
    await conn.execute('insert into foobar (foo) values (10)')
    await conn.execute('insert into foobar (foo) values (20)')
    await conn.execute('insert into foobar (foo) values (30)')

    stmt = await conn.prepare('select * from foobar')

    async def perform(i):
        async with conn.transaction():
            r = await stmt.execute(limit=1)
            assert r == [(10,)]
            r = await stmt.exec_continue(limit=1)
            assert r == [(20,)]

    async with trio.open_nursery() as nursery:
        nursery.start_soon(perform, 1)
        nursery.start_soon(perform, 2)

        await perform(3)

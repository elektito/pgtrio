import trio
from pytest import raises
from utils import postgres_socket_file, conn
from pgtrio import InterfaceError, ProgrammingError


async def test_simple(conn):
    await conn.execute('create table foobar (foo int)')

    stmt = await conn.prepare('insert into foobar (foo) values (10)')
    await stmt.execute()

    results = await conn.execute('select * from foobar')
    assert results == [(10,)]


async def test_params(conn):
    await conn.execute('create table foobar (foo int)')

    stmt = await conn.prepare('insert into foobar (foo) values ($1)')
    await stmt.execute(10)

    results = await conn.execute('select * from foobar')
    assert results == [(10,)]


async def test_param_descs(conn):
    stmt = await conn.prepare('select generate_series(0, $1)')
    param_descs = stmt.parameters
    assert len(param_descs) == 1
    assert param_descs[0].type == 'int4'

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

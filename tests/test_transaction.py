import trio
from pytest import raises, mark
from pgtrio import (
    PgIsolationLevel, PgReadWriteMode, InterfaceError, DatabaseError
)
from utils import postgres_socket_file, conn


isolation_levels = [
    PgIsolationLevel.SERIALIZABLE,
    PgIsolationLevel.REPEATABLE_READ,
    PgIsolationLevel.READ_COMMITTED,
    PgIsolationLevel.READ_UNCOMMITTED
]

rw_modes = [
    PgReadWriteMode.READ_WRITE,
    PgReadWriteMode.READ_ONLY,
]


@mark.parametrize('isolation', isolation_levels)
@mark.parametrize('rw_mode', [PgReadWriteMode.READ_WRITE])
@mark.parametrize('deferrable', [True, False])
async def test_transaction_normal(conn, isolation, rw_mode, deferrable):
    await conn.execute('create table foobar (foo int)')
    await conn.execute('insert into foobar (foo) values (10)')
    async with conn.transaction(isolation_level=isolation,
                                read_write_mode=rw_mode,
                                deferrable=deferrable):
        await conn.execute('insert into foobar (foo) values (20)')
    results = await conn.execute('select * from foobar')
    assert results == [(10,), (20,)]


async def test_read_only(conn):
    await conn.execute('create table foobar (foo int)')
    await conn.execute('insert into foobar (foo) values (10)')
    async with conn.transaction(
            read_write_mode=PgReadWriteMode.READ_ONLY):

        # this should go fine
        await conn.execute('select * from foobar')

        # but this shouldn't, since it tries to write
        with raises(DatabaseError):
            await conn.execute('insert into foobar (foo) values (20)')


async def test_transaction_error(conn):
    await conn.execute('create table foobar (foo int)')
    await conn.execute('insert into foobar (foo) values (10)')
    with raises(RuntimeError):
        async with conn.transaction():
            await conn.execute('insert into foobar (foo) values (20)')
            raise RuntimeError
    results = await conn.execute('select * from foobar')
    assert results == [(10,),]


async def test_transaction_explicit_commit(conn):
    await conn.execute('create table foobar (foo int)')
    await conn.execute('insert into foobar (foo) values (10)')
    async with conn.transaction() as tr:
        await conn.execute('insert into foobar (foo) values (20)')
        await tr.commit()

        # this will not be executed
        await conn.execute('insert into foobar (foo) values (30)')
    results = await conn.execute('select * from foobar')
    assert results == [(10,), (20,)]


async def test_transaction_explicit_rollback(conn):
    await conn.execute('create table foobar (foo int)')
    await conn.execute('insert into foobar (foo) values (10)')
    async with conn.transaction() as tr:
        await conn.execute('insert into foobar (foo) values (20)')
        await tr.rollback()

        # this will not be executed
        await conn.execute('insert into foobar (foo) values (30)')
    results = await conn.execute('select * from foobar')
    assert results == [(10,),]


async def test_savepoint_normal(conn):
    await conn.execute('create table foobar (foo int)')
    await conn.execute('insert into foobar (foo) values (10)')
    async with conn.transaction():
        await conn.execute('insert into foobar (foo) values (20)')
        async with conn.transaction():
            await conn.execute('insert into foobar (foo) values (30)')
        await conn.execute('insert into foobar (foo) values (40)')

    results = await conn.execute('select * from foobar')
    assert results == [(10,), (20,), (30,), (40,)]


async def test_savepoint_error(conn):
    await conn.execute('create table foobar (foo int)')
    await conn.execute('insert into foobar (foo) values (10)')
    async with conn.transaction():
        await conn.execute('insert into foobar (foo) values (20)')
        try:
            async with conn.transaction():
                await conn.execute('insert into foobar (foo) values (30)')
                raise RuntimeError
        except RuntimeError:
            pass
        await conn.execute('insert into foobar (foo) values (40)')

    results = await conn.execute('select * from foobar')
    assert results == [(10,), (20,), (40,)]


async def test_savepoint_rollback(conn):
    await conn.execute('create table foobar (foo int)')
    await conn.execute('insert into foobar (foo) values (10)')
    async with conn.transaction():
        await conn.execute('insert into foobar (foo) values (20)')
        try:
            async with conn.transaction() as sp:
                await conn.execute('insert into foobar (foo) values (30)')
                await sp.rollback()
                await conn.execute('insert into foobar (foo) values (35)')
        except RuntimeError:
            pass
        await conn.execute('insert into foobar (foo) values (40)')

    results = await conn.execute('select * from foobar')
    assert results == [(10,), (20,), (40,)]


async def test_savepoint_commit(conn):
    await conn.execute('create table foobar (foo int)')
    await conn.execute('insert into foobar (foo) values (10)')
    async with conn.transaction():
        await conn.execute('insert into foobar (foo) values (20)')
        try:
            async with conn.transaction() as sp:
                await conn.execute('insert into foobar (foo) values (30)')
                await sp.commit()
                await conn.execute('insert into foobar (foo) values (35)')
        except RuntimeError:
            pass
        await conn.execute('insert into foobar (foo) values (40)')

    results = await conn.execute('select * from foobar')
    assert results == [(10,), (20,), (30,), (40,)]


async def test_multiple_tasks(conn):
    await conn.execute('create table foobar (foo int)')

    async def task(i):
        async with conn.transaction():
            await conn.execute(
                'insert into foobar (foo) values ($1)', i + 0)
            await conn.execute(
                'insert into foobar (foo) values ($1)', i + 1)
            await conn.execute(
                'insert into foobar (foo) values ($1)', i + 2)

            async with conn.transaction():
                await conn.execute(
                    'insert into foobar (foo) values ($1)', i + 3)
                async with conn.transaction():
                    await conn.execute(
                        'insert into foobar (foo) values ($1)', i + 4)
                    await conn.execute(
                        'insert into foobar (foo) values ($1)', i + 5)
                    async with conn.transaction() as sp:
                        await conn.execute(
                            'insert into foobar (foo) values ($1)',
                            1000)
                        await sp.rollback()
                await conn.execute(
                    'insert into foobar (foo) values ($1)', i + 6)

            await conn.execute(
                'insert into foobar (foo) values ($1)', i + 7)
            await conn.execute(
                'insert into foobar (foo) values ($1)', i + 8)
            await conn.execute(
                'insert into foobar (foo) values ($1)', i + 9)

    async with trio.open_nursery() as nursery:
        for i in range(10):
            nursery.start_soon(task, i * 10)

    results = await conn.execute('select * from foobar')
    results = [i[0] for i in results]
    assert set(results) == set(range(100))


async def test_mix_manual(conn):
    await conn.execute('begin')
    with raises(InterfaceError):
        async with conn.transaction():
            pass

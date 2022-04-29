from pytest import raises, mark
from pgtrio import PgIsolationLevel, PgReadWriteMode
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
@mark.parametrize('rw_mode', rw_modes)
@mark.parametrize('deferrable', [True, False])
async def test_transaction_normal(conn, isolation, rw_mode, deferrable):
    await conn.execute('create table foobar (foo int)')
    await conn.execute('insert into foobar (foo) values (10)')
    async with conn.transaction(isolation_level=isolation):
        await conn.execute('insert into foobar (foo) values (20)')
    results = await conn.execute('select * from foobar')
    assert results == [(10,), (20,)]


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

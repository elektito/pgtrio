import trio
import pgtrio
from pytest import raises


async def test_simple(pool):
    async with pool.acquire() as conn:
        await conn.execute('create table foobar (foo int)')
        await conn.execute('insert into foobar (foo) values ($1)', 10)
        await conn.execute('insert into foobar (foo) values ($1)', 20)
        await conn.execute('insert into foobar (foo) values ($1)', 30)
        results = await conn.execute('select * from foobar')
        assert results == [(10,), (20,), (30,)]


async def test_concurrent_queries(pool):
    async with pool.acquire() as conn:
        await conn.execute(
            'create table foobar (foo int unique, bar int not null)')
        await conn.execute(
                'insert into foobar (foo, bar) values ($1, $2)',
                101, 201)

    async def successful_query():
        async with pool.acquire() as conn:
            results = await conn.execute('select * from foobar')
            assert results == [(101, 201)]

    async def invalid_query():
        async with pool.acquire() as conn:
            with raises(pgtrio.DatabaseError):
                await conn.execute('foo bar')

    async def failing_query():
        async with pool.acquire() as conn:
            with raises(pgtrio.DatabaseError):
                # violate unique and not-null constraints
                await conn.execute(
                    'insert into foobar (foo) values (101)')

    async with trio.open_nursery() as nursery:
        nursery.start_soon(invalid_query)
        nursery.start_soon(successful_query)
        nursery.start_soon(failing_query)


async def test_prepared_stmt_concurrent(pool):
    async with pool.acquire() as conn:
        await conn.execute('create table foobar (foo int)')
        await conn.execute('insert into foobar (foo) values (10)')
        await conn.execute('insert into foobar (foo) values (20)')
        await conn.execute('insert into foobar (foo) values (30)')

    async def perform(i):
        async with pool.acquire() as conn:
            stmt = await conn.prepare('select * from foobar')
            async with conn.transaction() as tr:
                r = await stmt.execute(limit=1)
                assert r == [(10,)]
                r = await stmt.exec_continue(limit=1)
                assert r == [(20,)]

    async with trio.open_nursery() as nursery:
        nursery.start_soon(perform, 1)
        nursery.start_soon(perform, 2)

        await perform(3)


async def test_concurrent_transactions(pool):
    async with pool.acquire() as conn:
        await conn.execute('create table foobar (foo int)')

    async def task(i):
        async with pool.acquire() as conn:
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

    async with pool.acquire() as conn:
        results = await conn.execute('select * from foobar')
        results = [i[0] for i in results]
        assert set(results) == set(range(100))

# pgtrio

This is a [Trio][1]-native PostgreSQL interface library. It implements
the PostgreSQL wire protocol (both text and binary) in pure Python, so
it's likely not the best performing interface library out there.

## Usage

To use pgtrio, you either start by calling the `connect` function or
the `create_pool`. The former returns a single `Connection` object
while the latter returns a `Pool` object that can be used to acquire
multiple `Connection` objects.

```python3
import trio
import pgtrio

async def main():
    async with pgtrio.connect('test') as conn:
        results = await conn.execute('select name, dob from users')
        for name, dob in results:
            print(name, dob)

trio.run(main)
```

A `Connection` object can be used to execute queries, or create
transactions, cursors or prepared statements. One `Connection` object
can only be used from a single Trio task. If you have multiple tasks,
you can use a `Pool` object.

```python3
import trio
import pgtrio

async def insert_rows(pool, start, end):
    async with pool.acquire() as conn:
        for i in range(start, end):
            results = await conn.execute('insert into numbers (n) values ($1)', i)

async def main():
    async with trio.open_nursery() as nursery:
        nursery.start_soon(perform, pool, 0, 10)
        nursery.start_soon(perform, pool, 10, 20)

trio.run(main)
```

As you can see, you can also use query parameters and pass your
arguments to the `execute` method after the query. `pgtrio`
automatically converts Python types to Postgres types. To see a list
of supported types, see the relevant section further in this document.

### Transactions

In order to create a transaction, you can use the
`Connection.transaction` method:

```python3
import trio
import pgtrio

async def main():
    async with pgtrio.connect('test') as conn:
        async with conn.transaction() as tr:
            conn.execute("insert into users (name) values ('John Smith')")
            conn.execute("insert into users (name) values ('Jane Smith')")

trio.run(main)
```

The transaction will be committed automatically when execution reaches
the end of the `async with` block. You can manually commit or rollback
the transaction at any point by calling `await tr.commit()` or `await
tr.rollback` at any point in the block. After any of those methods is
called, execution of the block will stop.

### Prepared Statements

If you need to execute a single query multiple times, perhaps with
different arguments each time, you can use prepared statements.

```python3
import trio
import pgtrio

async def main():
    async with pgtrio.connect('test') as conn:
        stmt = await conn.prepare('insert into numbers (n) values ($1)')
        await stmt.execute(100)
        await stmt.execute(200)

        numbers = await conn.execute('select n from numbers order by n')
        assert numbers == [(100,), (200,)]

trio.run(main)
```

### Cursors

`pgtrio` also supports cursors for fetching large numbers of rows
without loading them all in memory.

```python3
import trio
import pgtrio

async def main():
    async with pgtrio.connect('test') as conn:
        async with conn.transaction():
            cur = await conn.cursor('select * from users')
            chunk1 = cur.fetch(100)
            cur.forward(50)
            chunk2 = cur.fetch(100)

trio.run(main)
```

Notice that a cursor must be used in a transaction block.

The `fetch` method fetches the number of requested rows. The `forward`
method skips the given number of rows.

You can also obtain a cursor from a prepared statement.

```python3
import trio
import pgtrio

async def main():
    async with pgtrio.connect('test') as conn:
        async with conn.transaction():
            stmt = await conn.prepare('select * from users')
            cur = await stmt.cursor()
            chunk1 = cur.fetch(100)
            cur.forward(50)
            chunk2 = cur.fetch(100)

trio.run(main)
```

Instead of using the `fetch` method you can also use iteration to read
from a cursor:

```python3
import trio
import pgtrio

async def main():
    async with pgtrio.connect('test') as conn:
        async with conn.transaction():
            cur = await conn.cursor('select * from users')
            async for name, dob in cur:
                print(name, dob)

trio.run(main)
```

## Supported Types

`pgtrio` can automatically convert between the following
python/postgres types:

| postgres type | python type             |
|---------------|-------------------------|
| bool          | bool                    |
| bytea         | bytes                   |
| char          | str                     |
| cidr          | IPv4Network/IPv6Network |
| date          | datetime.date           |
| float4        | float                   |
| float8        | float                   |
| inet          | IPv4Address/IPv6Address |
| interval      | datetime.timedelta      |
| json          | list/dict               |
| jsonb         | list/dict               |
| text          | str                     |
| time          | datetime.time           |
| timetz        | datetime.time           |
| timestamp     | datetime.datetime       |
| timestamptz   | datetime.datetime       |
| varchar       | str                     |


[1]: https://github.com/python-trio/trio


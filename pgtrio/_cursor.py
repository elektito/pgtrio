from ._exceptions import ProgrammingError


class Cursor:
    def __init__(self, stmt):
        self.stmt = stmt

    async def _init(self):
        if not self.stmt.conn.in_transaction:
            raise ProgrammingError(
                'Cursors can only be used inside transactions')

    async def fetch_row(self):
        rows = await self.fetch(1)
        if rows:
            return rows[0]
        else:
            return None

    async def fetch(self, n):
        if self.stmt.finished:
            return []
        return await self.stmt.exec_continue(limit=n)

    async def forward(self, n):
        if self.stmt.finished:
            return
        return await self.stmt.forward(n)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.stmt.finished:
            raise StopAsyncIteration

        result = await self.fetch_row()
        if result:
            return result

        raise StopAsyncIteration

    def __iter__(self):
        raise TypeError(
            'Cursor object can only be iterated asynchronously. '
            'Did you forget to use "async for"?')

import trio


class TransactionExit(BaseException):
    pass


class Transaction:
    def __init__(self, conn, *,
                 isolation_level=None,
                 read_write_mode=None,
                 deferrable=False):
        self.conn = conn
        self.isolation_level = isolation_level
        self.read_write_mode = read_write_mode
        self.deferrable = deferrable

    async def commit(self):
        await self._commit()
        raise TransactionExit

    async def rollback(self):
        await self._rollback()
        raise TransactionExit

    async def _commit(self):
        await self.conn.execute('commit')

    async def _rollback(self):
        await self.conn.execute('rollback')

    async def __aenter__(self):
        query = 'begin'
        if self.isolation_level:
            query += f' isolation level {self.isolation_level}'
        if self.read_write_mode:
            query += f' {self.read_write_mode}'
        if self.deferrable:
            query += f' deferrable'

        await self.conn.execute(query)

        return self

    async def __aexit__(self, extype, ex, tb):
        if extype and issubclass(extype, TransactionExit):
            # return True to signify we've handled the exception
            return True

        if extype is None:
            await self._commit()
        else:
            await self._rollback()

        # propagate exception (if any)
        return False

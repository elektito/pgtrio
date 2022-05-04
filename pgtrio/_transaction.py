import trio
from collections import defaultdict
from ._exceptions import InterfaceError


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

        self._savepoint_id = None

    async def commit(self):
        await self._commit()
        raise TransactionExit

    async def rollback(self):
        await self._rollback()
        raise TransactionExit

    async def _commit(self):
        if self._savepoint_id is None:
            await self.conn._execute_simple('commit')
        else:
            await self.conn.execute(
                f'release savepoint {self._savepoint_id}')

    async def _rollback(self):
        if self._savepoint_id is None:
            await self.conn._execute_simple('rollback')
        else:
            await self.conn._execute_simple(
                f'rollback to {self._savepoint_id}')

    async def __aenter__(self):
        if self.conn.current_transaction:
            # we're inside a transaction block in the same task.
            await self._start_savepoint()
        else:
            if self.conn.in_transaction:
                raise InterfaceError(
                    'Cannot start a transaction block inside a '
                    'manually started transaction.')
            await self._start_transaction()

        return self

    async def _start_transaction(self):
        query = 'begin'
        if self.isolation_level:
            query += f' isolation level {self.isolation_level}'
        if self.read_write_mode:
            query += f' {self.read_write_mode}'
        if self.deferrable:
            query += f' deferrable'

        await self.conn._execute_simple(query)

        self.conn.current_transaction = self

    async def _start_savepoint(self):
        self._savepoint_id = self.conn._get_unique_id('savepoint')
        query = f'savepoint {self._savepoint_id}'
        await self.conn._execute_simple(query)

    async def __aexit__(self, extype, ex, tb):
        if extype and issubclass(extype, TransactionExit):
            # return True to signify we've handled the exception
            return True

        if not self.conn.closed.is_set():
            if extype is None:
                await self._commit()
            else:
                await self._rollback()

            if not self._savepoint_id:
                self.conn.current_transaction = None

        # propagate exception (if any)
        return False

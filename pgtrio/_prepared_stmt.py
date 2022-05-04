from collections import namedtuple
from . import _pgmsg
from ._utils import get_exc_from_msg
from ._exceptions import ProgrammingError, InterfaceError
from ._transaction import Transaction
from ._cursor import Cursor


NO_EXECUTE = object()


ParameterDesc = namedtuple('ParameterDesc', ['name', 'type'])


class PreparedStatement:
    def __init__(self, conn, query):
        self.conn = conn
        self.query = query

        # even though the null character is valid UTF-8, we can't use
        # it in queries, because at the protocol level, the queries
        # are sent as zero-terminated strings.
        if '\x00' in self.query:
            raise ProgrammingError(
                'NULL character is not valid in PostgreSQL queries.')

        self._execute_started = False
        self._portal_closed = False

        self._initialized = False
        self._initial_transaction = None
        self._param_oids = None
        self._row_desc = None
        self._stmt_name = None
        self._query_row_count = None

    async def _init(self):
        if self._initialized:
            return

        if self.conn.closed.is_set():
            raise ProgrammingError('Connection is closed.')

        if not self.conn._pg_types_loaded.is_set():
            # the "if" is not technically necessary, but avoids an
            # extra trio checkpoint.
            await self.conn._pg_types_loaded.wait()

        try:
            await self._parse_query()
        finally:
            await self.conn._wait_for_ready()

        self._initialized = True

        return self

    async def _parse_query(self):
        self._stmt_name = self.conn._get_unique_id('stmt')

        parse_msg = _pgmsg.Parse(self._stmt_name, self.query)
        describe_stmt_msg = _pgmsg.Describe(b'S', self._stmt_name)
        sync_msg = _pgmsg.Sync()
        await self.conn._send_msg(
            parse_msg, describe_stmt_msg, sync_msg)

        while True:
            msg = await self.conn._get_msg(_pgmsg.ParseComplete,
                                           _pgmsg.ErrorResponse,
                                           _pgmsg.RowDescription,
                                           _pgmsg.ParameterDescription,
                                           _pgmsg.NoData)
            if isinstance(msg, _pgmsg.ErrorResponse):
                # DESCRIBE command _can_ return an error, but only if
                # the portal/prepared statement specified does not
                # exist. In our case, this should never happen.
                raise get_exc_from_msg(
                    msg,
                    desc_prefix=(
                        f'Error preparing statement: {self.query}\n   '
                    ),
                )
            elif isinstance(msg, _pgmsg.ParameterDescription):
                self._param_oids = msg.param_oids
            elif isinstance(msg, _pgmsg.RowDescription):
                # this description is incomplete (since protocol
                # format is only known after bind, but can still be
                # used to return the list of parameters and their
                # types in the "parameters" proptery)
                self._row_desc = msg.fields
                break
            elif isinstance(msg, _pgmsg.NoData):
                self._row_desc = None
                break

        msg = await self.conn._get_msg(_pgmsg.ReadyForQuery)
        await self.conn._handle_msg_ready_for_query(msg)

    @property
    def parameters(self):
        if not self._row_desc:
            return None

        return [
            ParameterDesc(
                name=name,
                type=self.conn._codec_helper._oid_to_name.get(type_oid)
            )
            for name, table_oid, column_attr_id, type_oid, type_len,
                type_modifier, format_code
            in self._row_desc
        ]

    @property
    def rowcount(self):
        return self._query_row_count

    async def cursor(self, *params, **kwargs):
        await self.execute(limit=NO_EXECUTE)
        cursor = Cursor(self, **kwargs)
        await cursor._init()
        return cursor

    async def execute(self, *params, limit=None):
        if not self._initialized:
            raise ProgrammingError(
                'PreparedStatement not initialized')

        if self.conn.closed.is_set():
            raise ProgrammingError('Connection is closed.')

        if not self.conn._pg_types_loaded.is_set():
            # the "if" is not technically necessary, but avoids an
            # extra trio checkpoint.
            await self._pg_types_loaded.wait()

        if (limit and limit is not NO_EXECUTE and
            not self.conn.in_transaction):
            # we don't allow this because the only use case for
            # "limit" is to use exec_continue later, but exec_continue
            # won't work outside a transaction (postgres will complain
            # that "portal does not exist")
            raise InterfaceError(
                'limit parameter not supported outside a transaction')

        if self._execute_started and not self._portal_closed:
            # close the previously running portal
            await self._close_portal()
            await self.conn._wait_for_ready()

        self._initial_transaction = self.conn.current_transaction
        try:
            results = await self._execute(*params, limit=limit)
        finally:
            await self.conn._wait_for_ready()

        return results

    async def _execute(self, *params, limit=None):
        self._execute_started = True
        self._portal_closed = False
        self._portal_name = self.conn._get_unique_id('portal')

        should_close = False
        protocol_format = self.conn.protocol_format

        enc_params = []
        for param, param_oid in zip(params, self._param_oids):
            param_value = self.conn._codec_helper.encode_value(
                param, param_oid,
                protocol_format=protocol_format)
            enc_params.append(param_value)

        bind_msg = _pgmsg.Bind(self._portal_name, self._stmt_name,
                               params=enc_params,
                               param_format_codes=[protocol_format],
                               result_format_codes=[protocol_format])
        describe_portal_msg = _pgmsg.Describe(b'P', self._portal_name)
        sync_msg = _pgmsg.Sync()
        if limit is not NO_EXECUTE:
            execute_msg = _pgmsg.Execute(self._portal_name,
                                         max_rows=limit or 0)
            msgs = [bind_msg, describe_portal_msg, execute_msg,
                    sync_msg]
        else:
            msgs = [bind_msg, describe_portal_msg, sync_msg]
        await self.conn._send_msg(*msgs)

        # sent messages, so no other query allowed until we have a
        # ReadyForQuery
        self.conn._is_ready = False

        results = []
        while True:
            msg = await self.conn._get_msg(
                _pgmsg.ErrorResponse,
                _pgmsg.BindComplete,
                _pgmsg.RowDescription,
                _pgmsg.NoData,
                _pgmsg.EmptyQueryResponse,
                _pgmsg.CommandComplete,
                _pgmsg.DataRow,
                _pgmsg.ErrorResponse,
                _pgmsg.PortalSuspended)
            if isinstance(msg, _pgmsg.ErrorResponse):
                raise get_exc_from_msg(
                    msg,
                    desc_prefix=(
                        f'Error executing statement: {self.query}\n   '
                    ),
                )
            elif isinstance(msg, _pgmsg.DataRow):
                row = self.conn._codec_helper.decode_row(
                    msg.columns, self._row_desc)
                results.append(row)
            elif isinstance(msg, _pgmsg.RowDescription):
                self._row_desc = msg.fields
                if limit is NO_EXECUTE:
                    break
            elif isinstance(msg, _pgmsg.NoData):
                should_close = True
                if limit is NO_EXECUTE:
                    break
            elif isinstance(msg, _pgmsg.EmptyQueryResponse):
                should_close = True
                break
            elif isinstance(msg, _pgmsg.CommandComplete):
                if msg.cmd_tag.startswith(b'SELECT'):
                    _, rows = msg.cmd_tag.split(b' ')
                    self._query_row_count = int(rows.decode('ascii'))
                else:
                    self._query_row_count = None
                should_close = True
                break
            elif isinstance(msg, _pgmsg.PortalSuspended):
                break

        if should_close:
            await self.conn._wait_for_ready()
            await self._close_portal()

        return results

    async def exec_continue(self, limit=None):
        if self.conn.closed.is_set():
            raise ProgrammingError('Connection is closed.')

        if not self._execute_started:
            raise ProgrammingError(
                'exec_continue can only be called after execute is '
                'called')

        if not self.conn.in_transaction:
            # we don't allow this because outside a transaction
            # postgres will complain that "portal does not exist"
            raise InterfaceError(
                'Cannot continue execution of a statement outside '
                'a transaction')

        if self.conn.current_transaction != self._initial_transaction:
            raise ProgrammingError(
                'Cannot continue execution from another transaction')

        try:
            results = await self._exec_continue(limit=limit)
        finally:
            await self.conn._wait_for_ready()

        return results

    async def _exec_continue(self, limit=None):
        execute_msg = _pgmsg.Execute(self._portal_name, max_rows=limit)
        sync_msg = _pgmsg.Sync()
        await self.conn._send_msg(execute_msg, sync_msg)

        self.conn._is_ready = False

        results = []
        should_close = False
        while True:
            msg = await self.conn._get_msg(
                _pgmsg.ErrorResponse,
                _pgmsg.DataRow,
                _pgmsg.CommandComplete,
                _pgmsg.PortalSuspended)
            if isinstance(msg, _pgmsg.DataRow):
                row = self.conn._codec_helper.decode_row(
                    msg.columns, self._row_desc)
                results.append(row)
            elif isinstance(msg, _pgmsg.ErrorResponse):
                raise get_exc_from_msg(
                    msg,
                    desc_prefix=(
                        f'Error executing statement: {self.query}\n   '
                    ),
                )
            elif isinstance(msg, _pgmsg.CommandComplete):
                if msg.cmd_tag.startswith(b'SELECT'):
                    _, rows = msg.cmd_tag.split(b' ')
                    self._query_row_count = int(rows.decode('ascii'))
                else:
                    self._query_row_count = None
                should_close = True
                break
            elif isinstance(msg, _pgmsg.PortalSuspended):
                break
            else:
                assert False

        if should_close:
            await self.conn._wait_for_ready()
            await self._close_portal()

        return results

    async def _close_portal(self):
        if not self._execute_started or self._portal_closed:
            return

        close_msg = _pgmsg.Close(b'P', self._portal_name)
        sync_msg = _pgmsg.Sync()
        await self.conn._send_msg(close_msg, sync_msg)

        self.conn._is_ready = False

        # handle close portal response
        msg = await self.conn._get_msg(_pgmsg.CloseComplete,
                                       _pgmsg.ErrorResponse)
        if isinstance(msg, _pgmsg.ErrorResponse):
            raise get_exc_from_msg(
                msg,
                desc_prefix=(
                    f'Error closing portal: {self.query}\n   '
                ),
            )

        self._portal_closed = True

    async def forward(self, n):
        if self.conn.closed.is_set():
            raise ProgrammingError('Connection is closed.')

        if not self._execute_started:
            raise ProgrammingError(
                'exec_continue can only be called after execute is '
                'called')

        if not self.conn.in_transaction:
            # we don't allow this because outside a transaction
            # postgres will complain that "portal does not exist"
            raise InterfaceError(
                'Cannot continue execution of a statement outside '
                'a transaction')

        if self.conn.current_transaction != self._initial_transaction:
            raise ProgrammingError(
                'Cannot continue execution from another transaction')


        await self.conn.execute(
            f'move forward {n} from {self._portal_name}')

    def __repr__(self):
        return f'<PreparedStatement name={self._stmt_name} query="{self.query}">'

    def __del__(self):
        if hasattr(self, '_stmt_name') and self._stmt_name:
            self.conn._close_stmt(self._stmt_name)

    @property
    def finished(self):
        return self._portal_closed

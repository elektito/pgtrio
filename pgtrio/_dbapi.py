import logging
import warnings
import trio
from enum import Enum
from contextlib import asynccontextmanager
from functools import wraps
from collections import defaultdict
from . import _pgmsg
from ._codecs import CodecHelper
from ._utils import PgProtocolFormat, get_exc_from_msg
from ._transaction import Transaction
from ._prepared_stmt import PreparedStatement
from ._exceptions import (
    InternalError, DatabaseError, OperationalError, ProgrammingError,
)

DEFAULT_PG_UNIX_SOCKET = '/var/run/postgresql/.s.PGSQL.5432'
BUFFER_SIZE = 204800

logger = logging.getLogger(__name__)


class QueryStatus(Enum):
    INITIALIZING = 1    # still initializing and the first
                        # ReadyForQuery message is yet to arrive

    IDLE = 2            # connection is idle; we can send a query

    IN_TRANSACTION = 3  # we're inside a transaction block

    ERROR = 4           # current transaction has encountered an
                        # error; we need to rollback to exit the
                        # transaction


class Connection:
    def __init__(self, database, *,
                 unix_socket_path=None,
                 host=None,
                 port=None,
                 username=None,
                 password=None,
                 ssl=True,
                 ssl_required=True,
                 protocol_format=PgProtocolFormat._DEFAULT):
        self.database = database
        self.unix_socket_path = unix_socket_path
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.ssl = ssl
        self.protocol_format = PgProtocolFormat.convert(protocol_format)

        self._stream = None
        self._codec_helper = CodecHelper()

        self._nursery = None
        self._server_vars = {}
        self._notices = []
        self._id_counters = defaultdict(int)

        self._query_status = QueryStatus.INITIALIZING
        self._query_row_count = None
        self._query_results = []
        self._query_lock = trio.Lock()
        self._query_parse_phase_started = False
        self._query_describe_stmt_phase_started = False
        self._query_bind_phase_started = False
        self._query_describe_portal_phase_started = False
        self._query_execute_phase_started = False
        self._query_close_phase_started = False

        self._transaction_lock = trio.Lock()

        self._is_ready = False
        self._is_ready_cv = trio.Condition()

        self._run_send_finished = False

        # these channels are used to communicate data to be sent to
        # the back-end to the _run_send task
        self._outgoing_send_chan, self._outgoing_recv_chan = \
            trio.open_memory_channel(0)

        # these channels are used to communicate incoming messages to
        # whoever is interested (used by _get_msg)
        self._incoming_send_chan, self._incoming_recv_chan = None, None

        # this is set when we receive AuthenticationOk from postgres
        self._auth_ok = trio.Event()

        # this is set by the close() method
        self._closed = trio.Event()

        # this is set when postgres type info is loaded from the
        # pg_catalog.pg_type table
        self._pg_types_loaded = trio.Event()

    @property
    def server_vars(self):
        return self._server_vars

    @property
    def notices(self):
        return self._notices

    @property
    def rowcount(self):
        return self._query_row_count

    @property
    def in_transaction(self):
        return self._query_status == QueryStatus.IN_TRANSACTION

    def register_codec(self, codec):
        self._codec_helper.register_codec(codec)

    async def execute(self, query, *params):
        if self._closed.is_set():
            raise ProgrammingError('Connection is closed.')

        if not self._pg_types_loaded.is_set():
            # the "if" is not technically necessary, but avoids an
            # extra trio checkpoint.
            await self._pg_types_loaded.wait()

        stmt = await self.prepare(query)
        results = await stmt.execute(*params)
        self._query_row_count = stmt.rowcount

        return results

    async def _execute_simple(self, query,
                              dont_decode_values=False,
                              protocol_format=None):
        # even though the null character is valid UTF-8, we can't use
        # it in queries, because at the protocol level, the queries
        # are sent as zero-terminated strings.
        if '\x00' in query:
            raise ProgrammingError(
                'NULL character is not valid in PostgreSQL queries.')

        async with self._is_ready_cv:
            while not self._is_ready:
                await self._is_ready_cv.wait()

        if protocol_format is None:
            protocol_format = self.protocol_format

        async with self._query_lock:
            try:
                results = await self._process_simple_query(
                    query,
                    dont_decode_values=dont_decode_values,
                    protocol_format=protocol_format)
            finally:
                await self._wait_for_ready()

        return results

    def close(self):
        self._closed.set()

    def transaction(self, isolation_level=None, read_write_mode=None,
                    deferrable=False):
        return Transaction(self,
                           isolation_level=isolation_level,
                           read_write_mode=read_write_mode,
                           deferrable=deferrable)

    async def cursor(self, query, *params, **kwargs):
        stmt = await self.prepare(query)
        return await stmt.cursor(*params, **kwargs)

    def prepare(self, query):
        return PreparedStatement(self, query)

    async def _run(self):
        await self._connect()

        async with trio.open_nursery() as nursery, self._stream:
            self._nursery = nursery

            nursery.start_soon(self._run_recv)
            nursery.start_soon(self._run_send)

            msg = _pgmsg.StartupMessage(self.username, self.database)
            await self._send_msg(msg)

            nursery.start_soon(self._load_pg_types)

            await self._closed.wait()

            # Doing this before sending Terminate message makes sure
            # there's no conflict with other tasks
            nursery.cancel_scope.cancel()

            # make sure _send_run is finished so we won't get an error
            # by concurrently writing to the stream
            await _self._run_send_finished.wait()

            # attempt a graceful shutdown by sending a Terminate
            # message, but we can't use self._send_msg because the
            # send task is now canceled.
            msg = _pgmsg.Terminate()
            await self._stream.send_all(bytes(msg))

    async def _run_send(self):
        try:
            async for msg in self._outgoing_recv_chan:
                await self._stream.send_all(bytes(msg))
        finally:
            self._run_send_finished = True

    async def _run_recv(self):
        buf = b''
        while True:
            try:
                data = await self._stream.receive_some(BUFFER_SIZE)
            except trio.ClosedResourceError:
                break

            if data == b'':
                raise OperationalError('Database connection broken')
            buf += data

            start = 0
            while True:
                msg, length = _pgmsg.PgMessage.deserialize(buf, start)
                if msg is None:
                    break
                logger.debug('Received PG message: {msg}')

                if self._incoming_send_chan:
                    await self._incoming_send_chan.send(msg)
                else:
                    await self._handle_unsolicited_msg(msg)

                start += length

            buf = buf[start:]

    def _close_stmt(self, stmt_name):
        # this function is called by the __del__ method of a
        # PreparedStatement to deallocate the statement in the
        # background. The function itself cannot execute this since
        # it's not async.

        async def close_stmt():
            if not self._closed.is_set():
                try:
                    await self.execute(f'deallocate {stmt_name}')
                except trio.ClosedResourceError:
                    pass

        if not self._closed.is_set() and \
           not self._nursery.cancel_scope.cancelled_caught:
            self._nursery.start_soon(close_stmt)

    async def _wait_for_ready(self):
        if not self._is_ready:
            try:
                msg = await self._get_msg(_pgmsg.ReadyForQuery)
                await self._handle_msg_ready_for_query(msg)
                self._is_ready = True
            except:
                # can't salvage connection at this point
                self.close()
                raise

    async def _process_simple_query(self, query,
                                    dont_decode_values,
                                    protocol_format):
        msg = _pgmsg.Query(query)
        await self._send_msg(msg)

        results = []
        row_desc = None
        while True:
            msg = await self._get_msg(
                _pgmsg.ErrorResponse,
                _pgmsg.CommandComplete,
                _pgmsg.DataRow,
                _pgmsg.RowDescription,
                _pgmsg.EmptyQueryResponse)
            if isinstance(msg, _pgmsg.ErrorResponse):
                raise get_exc_from_msg(
                    msg,
                    desc_prefix=(
                        f'Error executing query: {query}\n   '
                    ),
                )
            elif isinstance(msg, _pgmsg.DataRow):
                if not dont_decode_values:
                    row = self._codec_helper.decode_row(
                        msg.columns, row_desc)
                    results.append(row)
                else:
                    results.append(msg.columns)
            elif isinstance(msg, _pgmsg.EmptyQueryResponse):
                self._query_row_count = 0
                break
            elif isinstance(msg, _pgmsg.RowDescription):
                row_desc = msg.fields
            elif isinstance(msg, _pgmsg.CommandComplete):
                if msg.cmd_tag.startswith(b'SELECT'):
                    _, rows = msg.cmd_tag.split(b' ')
                    self._query_row_count = int(rows.decode('ascii'))
                else:
                    self._query_row_count = None
                break
            else:
                assert False

        return results

    async def _get_msg(self, *msg_types):
        async for msg in self._incoming_recv_chan:
            if type(msg) in msg_types:
                return msg
            else:
                await self._handle_unsolicited_msg(msg)

    async def _send_msg(self, *msgs):
        query_message_types = (
            _pgmsg.Query,
            _pgmsg.Parse,
            _pgmsg.Bind,
            _pgmsg.Describe,
            _pgmsg.Close,
            _pgmsg.Flush,
            _pgmsg.Execute,
        )
        if any(isinstance(msg, query_message_types) for msg in msgs):
            # when one of these messages is sent, connection is not
            # ready anymore until we get a ReadyForQuery message
            self._is_ready = False

        data = b''.join(bytes(msg) for msg in msgs)
        await self._outgoing_send_chan.send(data)

    async def _handle_unsolicited_msg(self, msg):
        if isinstance(msg, _pgmsg.NoticeResponse):
            await self._handle_notice(msg)
            return

        if not self._auth_ok.is_set():
            await self._handle_pre_auth_msg(msg)
            return

        handler = {
            _pgmsg.BackendKeyData: self._handle_msg_backend_key_data,
            _pgmsg.ErrorResponse: self._handle_error,
            _pgmsg.ParameterStatus: self._handle_msg_parameter_status,
            _pgmsg.ReadyForQuery: self._handle_msg_ready_for_query,
        }.get(type(msg))
        if not handler:
            raise InternalError(
                f'Unexpected unsolicited message type: {msg}')
        await handler(msg)

    async def _handle_pre_auth_msg(self, msg):
        if isinstance(msg, _pgmsg.AuthenticationOk):
            self._auth_ok.set()
            logger.info('Authentication okay.')
            return

        if isinstance(msg, _pgmsg.AuthenticationMD5Password):
            logger.info(
                'Received request for MD5 password. Sending '
                'password...')
            msg = _pgmsg.PasswordMessage(
                self.password,
                md5=True,
                username=self.username,
                salt=msg.salt,
            )
            await self._stream.send_all(bytes(msg))
            return

    async def _handle_error(self, msg):
        raise get_exc_from_msg(msg)

    async def _handle_notice(self, msg):
        fields = dict(msg.pairs)

        notice_msg = fields.get('M')
        if notice_msg is not None:
            notice_msg = str(notice_msg)

        severity = fields.get('S')
        if severity is not None:
            severity = str(severity)

        self.notices.append((severity, notice_msg))
        log_msg = f'Received notice from backend: [{severity}] {notice_msg}'
        logger.info(log_msg)
        warnings.warn(log_msg)

    async def _handle_msg_backend_key_data(self, msg):
        self._backend_pid = msg.pid
        self._backend_secret_key = msg.secret_key
        logger.debug(
            f'Received backend key data: pid={msg.pid} '
            f'secret_key={msg.secret_key}')

    async def _handle_msg_command_complete(self, msg):
        if msg.cmd_tag.startswith(b'SELECT'):
            _, rows = msg.cmd_tag.split(b' ')
            self._query_row_count = int(rows.decode('ascii'))

    async def _handle_msg_parameter_status(self, msg):
        self._server_vars[msg.param_name] = msg.param_value

    async def _handle_msg_ready_for_query(self, msg):
        logger.debug('Backend is ready for query.')
        self._query_status = {
            b'I': QueryStatus.IDLE,
            b'T': QueryStatus.IN_TRANSACTION,
            b'E': QueryStatus.ERROR,
        }.get(msg.status)
        if self._query_status is None:
            raise InternalError(
                'Unknown status value in ReadyForQuery message: '
                f'{msg.status}')

        self._is_ready = True
        async with self._is_ready_cv:
            self._is_ready_cv.notify_all()

        if not self._incoming_send_chan:
            self._incoming_send_chan, self._incoming_recv_chan = \
                trio.open_memory_channel(0)

    async def _handle_msg_row_description(self, msg):
        self._row_desc = msg.fields

    async def _connect(self):
        if self.username is None:
            import getpass
            self.username = getpass.getuser()

        if self.password is None:
            self.password = ''

        try:
            if self.unix_socket_path:
                self._stream = await trio.open_unix_socket(
                    self.unix_socket_path)
            elif self.host:
                if not self.port:
                    self.port = 5432
                self._stream = await trio.open_tcp_stream(self.host, self.port)

                if self.ssl:
                    await self._setup_ssl()
            else:
                # try connecting to a default unix socket and then to
                # a default tcp port on localhost
                try:
                    self.unix_socket_path = DEFAULT_PG_UNIX_SOCKET
                    self._stream = await trio.open_unix_socket(
                        self.unix_socket_path)
                except (OSError, RuntimeError):
                    self.host = 'localhost'
                    if not self.port:
                        self.port = 5432
                    self._stream = await trio.open_tcp_stream(
                        self.host, self.port)
        except (trio.socket.gaierror, OSError) as e:
            raise OperationalError(str(e))

    async def _setup_ssl(self):
        await self._stream.send_all(bytes(_pgmsg.SSLRequest()))
        resp = await self._stream.receive_some(1)
        if resp == b'':
            raise OperationalError('Database connection broken')
        if resp == b'N':
            if self.ssl_required:
                raise OperationalError(
                    'Database server refused SSL request')
            return
        if resp != b'S':
            raise InternalError(
                'Received unexpected response to SSL request')

        import ssl
        self._stream = trio.SSLStream(
            self._stream,
            ssl.create_default_context()
        )

    async def _load_pg_types(self):
        results = await self._execute_simple(
            'select typname, oid from pg_catalog.pg_type',
            dont_decode_values=True,
            protocol_format=PgProtocolFormat.TEXT)
        self._codec_helper.init(results)
        self._pg_types_loaded.set()

    def _get_unique_id(self, id_type):
        self._id_counters[id_type] += 1
        idx = self._id_counters[id_type]
        return f'_pgtrio_{id_type}_{idx}'


@asynccontextmanager
@wraps(Connection)
async def connect(*args, **kwargs):
    conn = Connection(*args, **kwargs)
    async with trio.open_nursery() as nursery:
        nursery.start_soon(conn._run)

        async with conn._is_ready_cv:
            while not conn._is_ready:
                await conn._is_ready_cv.wait()

        yield conn

        conn.close()

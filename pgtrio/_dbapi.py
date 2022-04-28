import logging
import trio
from enum import Enum, IntEnum
from contextlib import asynccontextmanager
from functools import wraps
from . import _pgmsg
from ._codecs import CodecHelper
from ._exceptions import (
    Error, InternalError, DatabaseError, OperationalError,
    ProgrammingError
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


class PgProtocolFormat(IntEnum):
    TEXT = 0
    BINARY = 1

    _DEFAULT = BINARY

    @staticmethod
    def convert(value):
        if isinstance(value, PgProtocolFormat):
            return value
        elif isinstance(value, str):
            try:
                return PgProtocolFormat[value.upper()]
            except KeyError:
                pass

        raise ValueError(
            'Invalid protocol format value. A PgProtocolFormat value '
            'or its string representation is expected.')


class Query:
    def __init__(self, text, params, *,
                 dont_decode_values=False,
                 protocol_format=None):
        self.text = text
        self.params = params
        self.dont_decode_values = dont_decode_values
        self.protocol_format = protocol_format

        # even though the null character is valid UTF-8, we can't use
        # it in queries, because at the protocol level, the queries
        # are sent as zero-terminated strings.
        if '\x00' in self.text:
            raise ProgrammingError(
                'NULL character is not valid in PostgreSQL queries.')

    def __repr__(self):
        extra = []
        if self.dont_decode_values:
            extra.append('no_decode')
        if self.protocol_format is not None:
            extra.append(self.protocol_format.name.lower())
        if extra:
            extra = ' ' + ' '.join(extra)
        return f'<Query text="{self.text}"{extra}>'


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

        self._query_status = QueryStatus.INITIALIZING
        self._query_row_count = None
        self._query_results = []
        self._query_parse_phase_started = False
        self._query_bind_phase_started = False
        self._query_describe_phase_started = False
        self._query_execute_phase_started = False
        self._query_close_phase_started = False

        self._is_ready = False
        self._is_ready_cv = trio.Condition()

        # these channels are used to communicate data to be sent to
        # the back-end to the _run_send task
        self._outgoing_send_chan, self._outgoing_recv_chan = \
            trio.open_memory_channel(0)

        # these channels are used to send queries from the execute
        # method to the query processor
        self._query_send_chan, self._query_recv_chan = \
            trio.open_memory_channel(0)

        # these channels are used to send query results from the query
        # processor back to the execute method
        self._results_send_chan, self._results_recv_chan = \
            trio.open_memory_channel(0)

        # these will be set by process_query method to a pair of
        # send/receive channels and used to get messages from the
        # _run_recv task
        self._cur_msg_send_chan, self._cur_msg_recv_chan = None, None

        # this will be initialized to a new trio.Event when performing
        # a query, and the event is set when the results for that
        # query arrives
        self._have_query_results = None

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

    def register_codec(self, codec):
        self._codec_helper.register_codec(codec)

    async def execute(self, query, params={}):
        if not self._pg_types_loaded.is_set():
            # the "if" is not technically necessary, but avoids an
            # extra trio checkpoint.
            await self._pg_types_loaded.wait()

        return await self._execute(query, params)

    async def _execute(self, query, params={}, *,
                       dont_decode_values=False,
                       protocol_format=None):
        if self._closed.is_set():
            raise ProgrammingError('Connection is closed.')

        if protocol_format is None:
            protocol_format = self.protocol_format
        q = Query(query, params,
                  dont_decode_values=dont_decode_values,
                  protocol_format=protocol_format)
        self._have_query_results = trio.Event()
        await self._query_send_chan.send(q)
        results = await self._results_recv_chan.receive()
        if isinstance(results, Exception):
            raise results
        return results

    def close(self):
        self._closed.set()

    async def _run(self):
        await self._connect()

        async with trio.open_nursery() as nursery, self._stream:
            nursery.start_soon(self._run_recv)
            nursery.start_soon(self._run_send)
            nursery.start_soon(self._run_query_processor)

            msg = _pgmsg.StartupMessage(self.username, self.database)
            await self._send_msg(msg)

            nursery.start_soon(self._load_pg_types)

            await self._closed.wait()
            nursery.cancel_scope.cancel()

    async def _run_send(self):
        async for msg in self._outgoing_recv_chan:
            await self._stream.send_all(bytes(msg))

    async def _run_recv(self):
        buf = b''
        while True:
            data = await self._stream.receive_some(BUFFER_SIZE)
            if data == b'':
                raise OperationalError('Database connection broken')
            buf += data

            start = 0
            while True:
                msg, length = _pgmsg.PgMessage.deserialize(buf, start)
                if msg is None:
                    break
                logger.debug('Received PG message: {msg}')

                if self._cur_msg_send_chan:
                    await self._cur_msg_send_chan.send(msg)
                else:
                    await self._handle_unsolicited_msg(msg)

                start += length

            buf = buf[start:]

    async def _run_query_processor(self):
        async for query in self._query_recv_chan:
            self._cur_msg_send_chan, self._cur_msg_recv_chan = \
                trio.open_memory_channel(0)

            self._is_ready = False

            try:
                results = await self._process_query(query)
            except Error as e:
                results = e
            except:
                results = None
                raise
            finally:
                try:
                    await self._flush_extra_messages_after_query()
                except:
                    self.close()
                    raise

                self._cur_msg_send_chan = None
                self._cur_msg_recv_chan = None

            await self._results_send_chan.send(results)

    async def _flush_extra_messages_after_query(self):
        # in case we fail processing a query midways (maybe due to an
        # unexpected error), this routine ensures that we get to the
        # ready state again; if this function raises an exception, we
        # can't salvage the connection anymore and it must be closed.

        if self._is_ready:
            return

        if not self._query_execute_phase_started:
            self._is_ready = True
            return

        logger.debug(
            'Something bad happened during query execution. Continuing '
            'to consume and discard messages until we hit '
            'ReadyForQuery.')

        if not self._query_close_phase_started:
            msg = _pgmsg.Close(b'P', '')
            await self._send_msg(msg, _pgmsg.Sync())

        msg_types_to_discard = [
            _pgmsg.ParseComplete,
            _pgmsg.EmptyQueryResponse,
            _pgmsg.ErrorResponse,
            _pgmsg.BindComplete,
            _pgmsg.RowDescription,
            _pgmsg.NoData,
            _pgmsg.CommandComplete,
            _pgmsg.DataRow,
            _pgmsg.ErrorResponse,
            _pgmsg.PortalSuspended,
            _pgmsg.CloseComplete,
            _pgmsg.ReadyForQuery,
        ]
        while not self._is_ready:
            msg = await self._get_msg(*msg_types_to_discard)
            if isinstance(msg, _pgmsg.ReadyForQuery):
                await self._handle_msg_ready_for_query(msg)

    async def _process_query(self, query):
        self._cur_msg_send_chan, self._cur_msg_recv_chan = \
            trio.open_memory_channel(0)

        self._query_parse_phase_started = False
        self._query_bind_phase_started = False
        self._query_describe_phase_started = False
        self._query_execute_phase_started = False
        self._query_close_phase_started = False

        ## parse phase

        msg = _pgmsg.Parse('', query.text)
        await self._send_msg(msg, _pgmsg.Flush())

        self._query_parse_phase_started = True

        msg = await self._get_msg(_pgmsg.ParseComplete,
                                  _pgmsg.ErrorResponse)
        if isinstance(msg, _pgmsg.ErrorResponse):
            raise self._get_exc_from_msg(
                msg,
                desc_prefix=(
                    f'Error parsing query: {query.text}\n   '
                ),
            )
        assert isinstance(msg, _pgmsg.ParseComplete)

        ## bind phase

        if query.protocol_format is not None:
            protocol_format = query.protocol_format
        else:
            protocol_format = self.protocol_format
        msg = _pgmsg.Bind('', '',
                          result_format_codes=[protocol_format])
        await self._send_msg(msg, _pgmsg.Flush())

        self._query_bind_phase_started = True

        msg = await self._get_msg(_pgmsg.BindComplete,
                                  _pgmsg.ErrorResponse)
        if isinstance(msg, _pgmsg.ErrorResponse):
            raise self._get_exc_from_msg(
                msg,
                desc_prefix=(
                    f'Error binding query: {query.text}\n   '
                ),
            )
        assert isinstance(msg, _pgmsg.BindComplete)

        ## describe phase

        msg = _pgmsg.Describe(b'P', '')
        await self._send_msg(msg, _pgmsg.Flush())

        self._query_describe_phase_started = True

        msg = await self._get_msg(_pgmsg.RowDescription,
                                  _pgmsg.NoData,
                                  _pgmsg.ErrorResponse)
        if isinstance(msg, _pgmsg.ErrorResponse):
            # DESCRIBE command _can_ return an error, but only if the
            # portal/prepared statement specified does not exist. In
            # our case, this should never happen.
            raise InternalError(
                f'DESCRIBE command returned an error: {msg}')
        elif isinstance(msg, _pgmsg.RowDescription):
            row_desc = msg.fields
        else:
            # NoData is sent when the query will return no rows
            assert isinstance(msg, _pgmsg.NoData)

        ## execute phase

        msg = _pgmsg.Execute('')
        await self._send_msg(msg, _pgmsg.Flush())

        self._query_execute_phase_started = True

        results = []
        while msg := await self._get_msg(
                _pgmsg.EmptyQueryResponse,
                _pgmsg.CommandComplete,
                _pgmsg.DataRow,
                _pgmsg.ErrorResponse,
                _pgmsg.PortalSuspended):
            if isinstance(msg, _pgmsg.DataRow):
                if not query.dont_decode_values:
                    row = self._codec_helper.decode_row(
                        msg.columns, row_desc)
                    results.append(row)
                else:
                    results.append(msg.columns)
            elif isinstance(msg, _pgmsg.CommandComplete):
                if msg.cmd_tag.startswith(b'SELECT'):
                    _, rows = msg.cmd_tag.split(b' ')
                    self._query_row_count = int(rows.decode('ascii'))
                else:
                    self._query_row_count = None
                break
            elif isinstance(msg, _pgmsg.ErrorResponse):
                results = self._get_exc_from_msg(
                    msg,
                    desc_prefix=(
                        f'Error processing query: {query.text}\n   '
                    ),
                )
                break
            elif isinstance(msg, _pgmsg.EmptyQueryResponse):
                self._query_row_count = 0
                return []
            elif isinstance(msg, _pgmsg.PortalSuspended):
                raise InternalError(
                    'Encountered PortalSuspended but this should not '
                    'happen in current implementation, since we do not '
                    'set max_rows to a non-zero value.')
            else:
                assert False, 'This should not happen!'

        ## close phase

        # if there was an error during execute, we shouldn't close
        if not isinstance(results, Exception):
            msg = _pgmsg.Close(b'P', '')
            await self._send_msg(msg, _pgmsg.Sync())

            self._query_close_phase_started = True

            msg = await self._get_msg(_pgmsg.CloseComplete,
                                      _pgmsg.ErrorResponse)
            if isinstance(msg, _pgmsg.ErrorResponse):
                raise self._get_exc_from_msg(
                    msg,
                    desc_prefix=(
                        f'Error parsing query: {query.text}\n   '
                    ),
                )
            assert isinstance(msg, _pgmsg.CloseComplete)
        else:
            await self._send_msg(_pgmsg.Sync())

        msg = await self._get_msg(_pgmsg.ReadyForQuery,
                                  _pgmsg.ErrorResponse)
        if isinstance(msg, _pgmsg.ErrorResponse):
            raise self._get_exc_from_msg(
                msg,
                desc_prefix=(
                    f'Error parsing query: {query.text}\n   '
                ),
            )
        assert isinstance(msg, _pgmsg.ReadyForQuery)
        await self._handle_msg_ready_for_query(msg)

        return results

    async def _get_msg(self, *msg_types):
        async for msg in self._cur_msg_recv_chan:
            if type(msg) in msg_types:
                return msg
            else:
                await self._handle_unsolicited_msg(msg)

    async def _send_msg(self, *msgs):
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
        raise self._get_exc_from_msg(msg)

    def _get_exc_from_msg(self, msg, desc_prefix='', desc_suffix=''):
        fields = dict(msg.pairs)

        error_msg = fields.get('M')
        if error_msg is not None:
            error_msg = str(error_msg)

        severity = fields.get('S')
        if severity is not None:
            severity = str(severity)

        error_msg = desc_prefix + error_msg + desc_suffix

        return DatabaseError(
            error_msg=error_msg,
            severity=severity,
        )

    async def _handle_notice(self, msg):
        fields = dict(msg.pairs)

        notice_msg = fields.get('M')
        if error_msg is not None:
            error_msg = str(error_msg)

        severity = fields.get('S')
        if severity is not None:
            severity = str(severity)

        self.notices.append((severity, notice_msg))
        logger.info(
            'Received notice from backend: [{severity}] {notice_msg}')

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
        self._have_query_results.set()

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
        if self._query_status == QueryStatus.IDLE:
            self._is_ready = True
            async with self._is_ready_cv:
                self._is_ready_cv.notify()

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
        results = await self._execute(
            'select typname, oid from pg_catalog.pg_type',
            dont_decode_values=True,
            protocol_format=PgProtocolFormat.TEXT)
        self._codec_helper.init(results)
        self._pg_types_loaded.set()


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

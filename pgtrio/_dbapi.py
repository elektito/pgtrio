import logging
import trio
from contextlib import asynccontextmanager
from . import _pgmsg
from ._exceptions import InternalError, DatabaseError, OperationalError

DEFAULT_PG_UNIX_SOCKET = '/var/run/postgresql/.s.PGSQL.5432'
BUFFER_SIZE = 2048

logger = logging.getLogger(__name__)


class Connection:
    def __init__(self, database, *,
                 unix_socket_path=None,
                 host=None,
                 port=None,
                 username=None,
                 password=None,
                 ssl=True,
                 ssl_required=True):
        self.database = database
        self.unix_socket_path = unix_socket_path
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.ssl = ssl

        self._stream = None
        self._nursery = None
        self._server_vars = {}

        self._auth_ok = trio.Event()
        self._ready_for_query = trio.Event()

    @property
    def server_vars(self):
        return self._server_vars

    async def _run(self):
        await self._connect()

        msg = _pgmsg.StartupMessage(self.username, self.database)
        await self._stream.send_all(bytes(msg))

        buf = b''
        while True:
            received_data = await self._stream.receive_some(BUFFER_SIZE)
            if received_data == b'':
                raise OperationalError('Database connection broken')
            buf += received_data

            start = 0
            while True:
                msg, length = _pgmsg.PgMessage.deserialize(buf, start)
                if msg is None:
                    break
                logger.debug('Received PG message: {msg}')

                if isinstance(msg, _pgmsg.ErrorResponse):
                    await self._handle_error(msg)
                    continue

                if not self._auth_ok.is_set():
                    await self._handle_pre_auth_msg(msg)
                else:
                    await self._handle_msg(msg)

                start += length

            buf = buf[start:]

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

    async def _handle_msg(self, msg):
        handler = {
            _pgmsg.ParameterStatus: self._handle_msg_parameter_status,
            _pgmsg.BackendKeyData: self._handle_msg_backend_key_data,
            _pgmsg.ReadyForQuery: self._handle_msg_ready_for_query,
        }.get(type(msg))
        if not handler:
            raise InternalError(f'Unhandled message type: {msg}')
        await handler(msg)

    async def _handle_error(self, msg):
        fields = dict(msg.pairs)

        error_msg = fields.get('M')
        if error_msg is not None:
            error_msg = str(error_msg)

        severity = fields.get('S')
        if severity is not None:
            severity = str(severity)

        raise DatabaseError(
            error_msg=error_msg,
            severity=severity,
        )

    async def _handle_msg_parameter_status(self, msg):
        self._server_vars[msg.param_name] = msg.param_value

    async def _handle_msg_backend_key_data(self, msg):
        self._backend_pid = msg.pid
        self._backend_secret_key = msg.secret_key
        logger.debug(
            f'Received backend key data: pid={msg.pid} '
            f'secret_key={msg.secret_key}')

    async def _handle_msg_ready_for_query(self, msg):
        logger.debug('Backend is ready for query.')
        self._ready_for_query.set()

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
        print('setup ssl', self._stream)




@asynccontextmanager
async def connect(database, *,
                  unix_socket_path=None,
                  host=None,
                  port=None,
                  username=None,
                  password=None,
                  ssl=True,
                  ssl_required=True):
    conn = Connection(
        database,
        unix_socket_path=unix_socket_path,
        host=host,
        port=port,
        username=username,
        password=password,
        ssl=ssl,
        ssl_required=ssl_required,
    )
    async with trio.open_nursery() as nursery:
        nursery.start_soon(conn._run)
        await conn._ready_for_query.wait()

        yield conn

        nursery.cancel_scope.cancel()

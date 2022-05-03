import hashlib


#
# postgres message data types as defined here:
# https://www.postgresql.org/docs/current/protocol-message-types.html
#


class PgBaseDataType:
    pass


class _Int(int, PgBaseDataType):
    def __bytes__(self):
        return self.to_bytes(length=self._int_size,
                             byteorder='big',
                             signed=True)

    @classmethod
    def deserialize(cls, msg, start):
        value = msg[start:start+cls._int_size]
        value = int.from_bytes(value, byteorder='big', signed=True)
        return cls(value), cls._int_size


class Int8(_Int):
    _int_size = 1


class Int16(_Int):
    _int_size = 2


class Int32(_Int):
    _int_size = 4


class Byte1(bytes, PgBaseDataType):
    def __new__(cls, value):
        assert isinstance(value, bytes)
        assert len(value) == 1
        return super().__new__(cls, value)

    def __repr__(self):
        return f'<Byte1 {super().__repr__()} ({self[0]})>'

    @classmethod
    def deserialize(cls, msg, start):
        return cls(msg[start:start+1]), 1


class String(bytes, PgBaseDataType):
    def __new__(cls, value):
        if isinstance(value, str):
            value = value.encode('utf-8')
        assert isinstance(value, bytes)
        return super().__new__(cls, value)

    def __bytes__(self):
        return self + b'\0'

    def __repr__(self):
        return f'<String "{self}">'

    def __str__(self):
        try:
            value = self.decode('utf-8')
        except UnicodeDecodeError:
            value = super().__str__[1:]  # remove the b prefix
        return f'{value}'

    @classmethod
    def deserialize(cls, msg, start):
        try:
            null_idx = msg.index(b'\0', start)
        except ValueError:
            raise ValueError('String is not null terminated.')
        value = msg[start:null_idx]
        return cls(value), null_idx - start + 1


#
# postgres messages
#


# metaclass for all postgres message classes, applied through the base
# class PgMessage
class PgMessageMetaClass(type):
    def __new__(cls, name, bases, attrs, **kwargs):
        if name != 'PgMessage':
            klass = PgMessageMetaClass._create_class(
                cls, name, bases, attrs, **kwargs)
        else:
            klass = super().__new__(cls, name, bases, attrs)

        return klass

    @staticmethod
    def _create_class(cls, name, bases, attrs, **kwargs):
        if '_type' not in attrs:
            # allow _type to be inherited
            for base_class in bases:
                if hasattr(base_class, '_type'):
                    # don't use the _type here though, because we've
                    # processed it already for the base class
                    _type = None
                    break
            else:
                raise TypeError(
                    f'Class {name} missing class variable "_type"')
        else:
            _type = attrs['_type']

        if 'side' not in kwargs:
            raise TypeError(
                f'Class {name} missing class keyword argument "side".')
        side = kwargs['side']

        if _type is not None:
            if not isinstance(_type, bytes) or len(_type) != 1:
                raise ValueError(
                    '_type field should contain a bytes object of '
                    'size 1 or None.')

        if side not in ['backend', 'frontend', 'both']:
            raise ValueError(
                'Class argument "side" can only have one of these '
                'values: backend, frontend, both')

        # store the side argument as a class variable
        attrs['_side'] = side

        for attr, value in attrs.items():
            if attr.startswith('_'):
                continue
            if not isinstance(value, PgBaseDataType) and \
               not (isinstance(value, type) and
                    issubclass(value, PgBaseDataType)) and \
               not callable(value):
                raise TypeError(
                    'PgMessage sub-class fields should either by a '
                    'sub-class of PgBaseDataType, or an instance of '
                    'such a sub-class, or a callable returning '
                    'bytes.')

        klass = super().__new__(cls, name, bases, attrs)
        if _type is not None and side != 'frontend':
            PgMessage._msg_classes[_type] = klass

        return klass


# this is the base class for all postgres messages. it adds a
# __bytes__ function that serializes an instance of a sub-class to
# bytes.
#
# see list of messages here:
# https://www.postgresql.org/docs/current/protocol-message-formats.html
class PgMessage(metaclass=PgMessageMetaClass):
    # maps message types to their relevant sub-class of
    # PgMessage. this will be populated by PgMessageMetaClass.
    _msg_classes = {}

    def __bytes__(self):
        # calculate payload
        klass = type(self)
        payload = bytearray()
        for attr, field in vars(klass).items():
            if attr.startswith('_'):
                continue
            if isinstance(field, PgBaseDataType):
                # the field contains a concrete value (like Int32(40),
                # which should always contain the value 40)
                value = bytes(field)
            elif isinstance(field, type) and \
                 issubclass(field, PgBaseDataType):
                # the field contains just a type (like Int32); the
                # field value should have been set in the object
                # itself before serialization attempt.
                value = getattr(self, attr)
                if not isinstance(value, PgBaseDataType):
                    # case it to the relevant PgBaseDataType type
                    value = field(value)
            elif callable(field):
                value = field(self)
            payload += bytes(value)

        # add four to length due to the length of the "length" field
        # itself
        length = 4 + len(payload)
        length = length.to_bytes(length=4, byteorder='big', signed=True)

        msg = self._type if self._type is not None else b''
        msg += length + payload

        return msg

    @classmethod
    def deserialize(cls, msg, start=0):
        if len(msg) - start < 5:
            # not enough data
            return None, 0

        msg_type = msg[start + 0:start + 1]
        subclass = PgMessage._msg_classes.get(msg_type)
        if subclass is None:
            raise ValueError(f'Unknown message type: {msg_type}')

        msg_len = msg[start + 1:start + 5]
        msg_len = int.from_bytes(msg_len, byteorder='big')

        if msg_len > len(msg) - start - 1:
            # not enough data
            return None, 0

        msg = subclass._deserialize(
            msg,
            start + 1 + 4,  # one byte for type, 4 for length
            msg_len - 4     # length consists of the length field
                            # itself but not type
        )

        # return the deserialized message, as well as the number of
        # bytes consumed.
        return msg, msg_len + 1

    @classmethod
    def _deserialize(cls, msg, start, length):
        # this is the default implementation of the _deserialize
        # method that can deserialize all messages, except the ones
        # with unusual structure. for those, the _deserialize class
        # method should be overridden in the relevant sub-class.

        msg_obj = cls()
        idx = start
        for attr, value in vars(cls).items():
            if attr.startswith('_'):
                continue

            if isinstance(value, PgBaseDataType):
                # the field contains a concrete value (like Int32(40),
                # which should always contain the value 40)
                field_type = type(value)
            elif isinstance(value, type) and \
                 issubclass(value, PgBaseDataType):
                # the field contains just a type (like Int32)
                field_type = value
            elif callable(value):
                raise ValueError(
                    'Cannot deserialize messages with dynamic fields.')
            else:
                # the metaclass validation should prevent this to ever
                # happen
                assert False

            field_value, field_len = field_type.deserialize(msg, idx)
            idx += field_len

            setattr(msg_obj, attr, field_value)

        return msg_obj


# this class handles the following message types: AuthenticationOk,
# AuthenticationKerberosV5, AuthenticationCleartextPassword,
# AuthenticationMD5Password, AuthenticationSCMCredential,
# AuthenticationGSS, AuthenticationGSSContinue, AuthenticationSSPI,
# AuthenticationSASL, AuthenticationSASLContinue, AuthenticationSASLFinal.
#
# all of these messages have the same _type field (R). the deserialize
# method returns the appropriate sub-class, depending on the value of
# the auth field.
class Authentication(PgMessage, side='backend'):
    _type = b'R'
    auth = Int32(0)
    # depending on the value of the auth field, more fields might
    # follow; a custom _deserialize function will handle these.

    @classmethod
    def _deserialize(cls, msg, start, length):
        auth, _ = Int32.deserialize(msg, start)
        if auth == 0:
            return AuthenticationOk()
        if auth == 2:
            return AuthenticationKerberosV5()
        if auth == 3:
            return AuthenticationCleartextPassword()
        if auth == 5:
            salt = msg[start+4:start+8]
            return AuthenticationMD5Password(salt)
        if auth == 6:
            return AuthenticationSCMCredential()
        if auth == 7:
            return AuthenticationGSS()
        if auth == 8:
            auth_data = msg[start+4:]
            return AuthenticationGSSContinue(auth_data)
        if auth == 9:
            return AuthenticationSSPI()
        if auth == 10:
            sasl_auth_mechanism, _ = String.deserialize(msg, start + 4)
            return AuthenticationSASL(sasl_auth_mechanism)
        if auth == 11:
            sasl_data = msg[start+4:]
            return AuthenticationSASLContinue(sasl_data)
        if auth == 12:
            sasl_additional_data = msg[start+4:]
            return AuthenticationSASLFinal(sasl_additional_data)
        raise ValueError(f'Unknown auth type: {auth}')


class AuthenticationOk(Authentication, side='backend'):
    auth = Int32(0)

    def __repr__(self):
        return '<AuthenticationOk>'


class AuthenticationKerberosV5(Authentication, side='backend'):
    auth = Int32(2)

    def __repr__(self):
        return '<AuthenticationKerberosV5>'


class AuthenticationCleartextPassword(Authentication, side='backend'):
    auth = Int32(3)

    def __repr__(self):
        return '<AuthenticationCleartextPassword>'


class AuthenticationMD5Password(Authentication, side='backend'):
    auth = Int32(5)

    def __init__(self, salt):
        self.salt = salt

    def __repr__(self):
        salt = repr(self.salt)[2:-1]  # string b prefix and quotes
        return f'<AuthenticationMD5Password salt="{salt}">'


class AuthenticationSCMCredential(Authentication, side='backend'):
    auth = Int32(6)

    def __repr__(self):
        return '<AuthenticationSCMCredential>'


class AuthenticationGSS(Authentication, side='backend'):
    auth = Int32(7)

    def __repr__(self):
        return '<AuthenticationGSS>'


class AuthenticationGSSContinue(Authentication, side='backend'):
    auth = Int32(8)

    def __init__(self, auth_data):
        self.auth_data = auth_data

    def __repr__(self):
        auth_data = repr(self.auth_data)[2:-1]  # string b prefix and
                                                # quotes
        return f'<AuthenticationGSSContinue auth_data={auth_data}>'


class AuthenticationSSPI(Authentication, side='backend'):
    auth = Int32(9)

    def __repr__(self):
        return '<AuthenticationSSPI>'


class AuthenticationSASL(Authentication, side='backend'):
    auth = Int32(10)

    def __init__(self, auth_mechanism):
        self.auth_mechanism = auth_mechanism

    def __repr__(self):
        auth_mech = repr(self.auth_mechanism)[2:-1]  # string b prefix
                                                     # and quotes
        return f'<AuthenticationSASL auth_mechanism={auth_mech}>'


class AuthenticationSASLContinue(Authentication, side='backend'):
    auth = Int32(11)

    def __init__(self, data):
        self.data = data

    def __repr__(self):
        data = repr(self.data)[2:-1]  # string b prefix and quotes
        return f'<AuthenticationSASLContinue auth_mechanism={data}>'


class AuthenticationSASLFinal(Authentication, side='backend'):
    auth = Int32(12)

    def __init__(self, data):
        self.data = data

    def __repr__(self):
        data = repr(self.data)[2:-1]  # string b prefix and quotes
        return f'<AuthenticationSASLFinal auth_mechanism={data}>'


class BackendKeyData(PgMessage, side='backend'):
    _type = b'K'
    pid = Int32
    secret_key = Int32

    def __repr__(self):
        return (
            f'<BackendKeyData pid={self.pid} '
            f'secret_key={self.secret_key}>'
        )


class Bind(PgMessage, side='frontend'):
    _type = b'B'
    portal_name = String
    stmt_name = String
    param_format_code_count = Int16
    # if param_format_code_count is non-zero, a number of Int16's
    # follow, specifying format codes of parameters (0 means text,
    # which is the default, and 1 means binary)
    param_count = Int16
    # if param_count is non-zero, a number of (Int32, ByteN) pairs
    # appear, specifying the length and content of parameters.
    result_format_code_count = Int16
    # if result_format_code_count is non-zero, a number of Int16's
    # appear here, specifying the format codes of each column of
    # results.

    def __init__(self, portal_name, stmt_name, params=None,
                 param_format_codes=None, result_format_codes=None):
        if not isinstance(stmt_name, (str, bytes)):
            raise TypeError('stmt_name should be a bytes or str')
        if not isinstance(portal_name, (str, bytes)):
            raise TypeError('portal_name should be a bytes or str')
        self.portal_name = portal_name
        self.stmt_name = stmt_name
        self.params = params or []
        self.param_format_codes = param_format_codes or []
        self.result_format_codes = result_format_codes or []

    def __bytes__(self):
        portal_name = bytes(String(self.portal_name))
        stmt_name = bytes(String(self.stmt_name))
        param_format_code_count = bytes(
            Int16(len(self.param_format_codes)))
        param_format_codes = b''.join(
            bytes(Int16(fc)) for fc in self.param_format_codes)
        param_count = bytes(Int16(len(self.params)))
        params = b''.join(
            # NULL values are indicated by setting the length field to
            # -1
            (
                bytes(Int32(len(p) if p is not None else -1)) +
                (b'' if p is None else p)
            )
            for p in self.params
        )
        result_format_code_count = bytes(
            Int16(len(self.result_format_codes)))
        result_format_codes = b''.join(
            bytes(Int16(fc)) for fc in self.result_format_codes)
        length = bytes(Int32(
            4 +
            len(portal_name) +
            len(stmt_name) +
            len(param_format_code_count) +
            len(param_format_codes) +
            len(param_count) +
            len(params) +
            len(result_format_code_count) +
            len(result_format_codes)
        ))
        return (
            b'B' +
            length +
            portal_name +
            stmt_name +
            param_format_code_count +
            param_format_codes +
            param_count +
            params +
            result_format_code_count +
            result_format_codes
        )

    def __repr__(self):
        params = ''
        if self.params:
            params = f' params={self.params!r}'
        pcodes = ''
        if self.param_format_codes:
            pcodes = f' param_fmt={[int(c) for c in self.param_format_codes]!r}'
        rcodes = ''
        if self.result_format_codes:
            rcodes = f' result_fmt={[int(c) for c in self.result_format_codes]!r}'
        return (
            f'<Bind portal={self.portal_name} stmt={self.stmt_name}'
            f'{params}{pcodes}{rcodes}>'
        )


class BindComplete(PgMessage, side='backend'):
    _type = b'2'

    def __repr__(self):
        return '<BindComplete>'


class CommandComplete(PgMessage, side='backend'):
    _type = b'C'
    cmd_tag = String

    def __repr__(self):
        return f'<CommandComplete tag="{self.cmd_tag}">'


class Close(PgMessage, side='frontend'):
    _type = b'C'
    type = Byte1
    name = String

    def __init__(self, stmt_or_portal, name):
        assert stmt_or_portal in (b'S', b'P')
        self.type = stmt_or_portal

        if isinstance(name, str):
            name = name.encode('utf-8')
        self.name = name

    def __bytes__(self):
        type_bytes = bytes(self.type)
        name_bytes = bytes(String(self.name))
        length = bytes(Int32(4 + len(type_bytes) + len(name_bytes)))
        return b'C' + length + type_bytes + name_bytes

    def __repr__(self):
        type = self.type.decode('ascii')
        try:
            name = self.name.decode('ascii')
        except UnicodeDecodeError:
            name = repr(self.name)[2:-1]
        return f'<Close type={type} name={name}>'


class CloseComplete(PgMessage, side='backend'):
    _type = b'3'

    def __repr__(self):
        return '<CloseComplete>'


class DataRow(PgMessage, side='backend'):
    _type = b'D'
    column_count = Int16

    # for each column there will be an Int32, telling us the length of
    # the column value, and a ByteN containing the value.

    def __init__(self, columns=None):
        self.columns = columns
        if self.columns is None:
            self.columns = []

    def __repr__(self):
        columns = ' '.join(repr(c) for c in self.columns)
        return f'<DataRow {columns}>'

    @classmethod
    def _deserialize(cls, msg, start, length):
        obj = cls()

        idx = start
        obj.column_count, n = Int16.deserialize(msg, idx)
        idx += n

        for i in range(obj.column_count):
            column_size, n = Int32.deserialize(msg, idx)
            idx += n

            if column_size < 0:
                # this is a NULL value
                column_value = None
            else:
                column_value = msg[idx:idx+column_size]
                idx += column_size

            obj.columns.append(column_value)

        return obj


class Describe(PgMessage, side='frontend'):
    _type = b'D'
    type = Byte1
    name = String

    def __init__(self, stmt_or_portal, name):
        assert stmt_or_portal in (b'S', b'P')
        self.type = stmt_or_portal

        if isinstance(name, str):
            name = name.encode('utf-8')
        self.name = name

    def __bytes__(self):
        type_bytes = bytes(self.type)
        name_bytes = bytes(String(self.name))
        length = bytes(Int32(4 + len(type_bytes) + len(name_bytes)))
        return b'D' + length + type_bytes + name_bytes

    def __repr__(self):
        type = self.type.decode('ascii')
        try:
            name = self.name.decode('ascii')
        except UnicodeDecodeError:
            name = repr(self.name)[2:-1]
        return f'<Describe type={type} name={name}>'


class EmptyQueryResponse(PgMessage, side='backend'):
    _type = b'I'

    def __repr__(self):
        return '<EmptyQueryResponse>'


class ErrorResponse(PgMessage, side='backend'):
    _type = b'E'
    # fields consist of one or more of (Byte1, String) pairs

    def __init__(self, pairs):
        self.pairs = pairs

    def __repr__(self):
        nfields = f'({len(self.pairs)} field(s))'
        pairs = ' '.join(f'{code}={value}' for code, value in self.pairs)
        return f'<ErrorResponse {nfields} {pairs}>'

    @classmethod
    def _deserialize(cls, msg, start, length):
        pairs = []
        idx = start
        while True:
            if idx >= len(msg):
                raise ValueError(
                    'Unterminated ErrorResponse message (should end '
                    'with a zero byte)')
            code, n = Byte1.deserialize(msg, idx)
            if code == b'\0':
                break
            code = chr(code[0])  # convert e.g. b'C' to 'C'
            idx += n
            value, n = String.deserialize(msg, idx)
            idx += n
            pairs.append((code, value))
        obj = cls(pairs)
        return obj


class Execute(PgMessage, side='frontend'):
    _type = b'E'
    portal_name = String
    max_rows = Int32

    def __init__(self, portal_name, max_rows=None):
        self.portal_name = portal_name
        self.max_rows = max_rows or 0

    def __bytes__(self):
        portal_name = bytes(String(self.portal_name))
        max_rows = bytes(Int32(self.max_rows))
        length = bytes(Int32(4 + len(portal_name) + len(max_rows)))
        return b'E' + length + portal_name + max_rows

    def __repr__(self):
        max_rows = ''
        if self.max_rows:
            max_rows = f' max_rows={self.max_rows}'
        return f'<Execute portal={self.portal_name}{max_rows}>'


class Flush(PgMessage, side='frontend'):
    _type = b'H'


class NegotiateProtocolVersion(PgMessage, side='backend'):
    _type = b'v'
    minor_ver_supported = Int32
    n_unrecognized_proto_options = Int32
    option_name = String


class NoData(PgMessage, side='backend'):
    _type = b'n'

    def __repr__(self):
        return '<NoData>'


class NoticeResponse(PgMessage, side='backend'):
    _type = b'N'
    # the message body contains one or more of (Byte1, String) pairs
    # each denoting a code and a value. The custom _deserializer
    # gathers these in a field named "notices".

    def __init__(self, pairs=None):
        self.pairs = pairs or []

    def __repr__(self):
        nfields = f'({len(self.notices)} field(s))'
        notices = ' '.join(
            f'{code}={value}' for code, value in self.notices)
        return f'<NoticeMessage {nfields} {notices}>'

    @classmethod
    def _deserialize(cls, msg, start, length):
        pairs = []
        idx = start
        while True:
            if idx >= len(msg):
                raise ValueError(
                    'Unterminated NoticeMessage message (should end '
                    'with a zero byte)')
            code, n = Byte1.deserialize(msg, idx)
            if code == b'\0':
                break
            code = chr(code[0])  # convert e.g. b'C' to 'C'
            idx += n
            value, n = String.deserialize(msg, idx)
            idx += n
            pairs.append((code, value))
        obj = cls(pairs)
        return obj


class ParameterDescription(PgMessage, side='backend'):
    _type = b't'
    nparams = Int16
    # after this, for each parameter an oid value follows. these will
    # be gather in the param_oids field.

    def __init__(self, param_oids=[]):
        self.param_oids = []

    def __repr__(self):
        return f'<ParameterDescription {self.param_oids}>'

    @classmethod
    def _deserialize(cls, msg, start, length):
        obj = cls()
        idx = start
        obj.nparams, n = Int16.deserialize(msg, idx)
        idx += n
        while len(obj.param_oids) < obj.nparams:
            oid, n = Int32.deserialize(msg, idx)
            idx += n
            obj.param_oids.append(oid)
        return obj


class ParameterStatus(PgMessage, side='backend'):
    _type = b'S'
    param_name = String
    param_value = String

    def __repr__(self):
        return (
            f'<ParameterStatus name={self.param_name} '
            f'value={self.param_value}>'
        )


class Parse(PgMessage, side='frontend'):
    _type = b'P'
    name = String
    query = String
    param_type_count = Int16

    # if param_type_count is non-zero, a number of Int32's follow,
    # specifying the type of parameters

    def __init__(self, name, query, param_types=None):
        if isinstance(name, str):
            name = name.encode('utf-8')
        if isinstance(query, str):
            query = query.encode('utf-8')
        self.name = name
        self.query = query
        self.param_types = param_types or []

    def __bytes__(self):
        name = bytes(String(self.name))
        query = bytes(String(self.query))
        param_types = b''.join(
            bytes(Int32(pt)) for pt in self.param_types)
        param_type_count = bytes(Int16(len(self.param_types)))
        length = bytes(Int32(
            4 +
            len(name) +
            len(query) +
            len(param_type_count) +
            len(param_types)
        ))
        return (
            b'P' +
            length +
            name +
            query +
            param_type_count +
            param_types
        )

    def __repr__(self):
        name = self.name
        if isinstance(name, bytes):
            name = name.decode('utf-8')
        query = self.query
        if isinstance(query, bytes):
            query = query.decode('utf-8')
        return (
            f'<Parse name={name} query="{query}" '
            f'param_types={self.param_types}>'
        )


class ParseComplete(PgMessage, side='backend'):
    _type = b'1'

    def __repr__(self):
        return f'<ParseComplete>'


class PasswordMessage(PgMessage, side='frontend'):
    _type = b'p'
    password = String

    def __init__(self, password, *, md5=False, username=None,
                 salt=None):
        if isinstance(password, str):
            password = password.encode('utf-8')

        if md5:
            if salt is None:
                raise ValueError('salt is required for MD5 password')

            username = username or b''
            if isinstance(username, str):
                username = username.encode('utf-8')

            # calculate md5 hash as described here:
            # https://www.postgresql.org/docs/current/auth-password.html
            userpass = password + username
            userpass_md5 = (
                hashlib.md5(userpass)
                .hexdigest()
                .encode('ascii')
            )
            final_hash = (
                hashlib.md5(userpass_md5 + salt)
                .hexdigest()
                .encode('ascii')
            )
            password = b'md5' + final_hash

        self.password = password

    def __repr__(self):
        password = repr(bytes(self.password))[2:-1]
        return f'<PasswordMessage password="{password}">'


class PortalSuspended(PgMessage, side='backend'):
    _type = b's'


class Query(PgMessage, side='frontend'):
    _type = b'Q'
    query = String

    def __init__(self, query):
        if isinstance(query, str):
            query = query.encode('utf-8')
        if not isinstance(query, bytes):
            raise ValueError(
                'query should either by a str or bytes; got '
                f'{type(query)}')

        self.query = query

    def __repr__(self):
        query = self.query
        if isinstance(query, bytes):
            query = query.decode('utf-8')
        return f'<Query {query}>'


class ReadyForQuery(PgMessage, side='backend'):
    _type = b'Z'
    status = Byte1

    def __repr__(self):
        try:
            status = self.status.decode('utf-8')
        except UnicodeDecodeError:
            status = str(self.status)

        if status == 'I':
            status_desc = 'idle'
        elif status == 'T':
            status_desc = 'inside-transaction-block'
        elif status == 'E':
            status_desc = 'error'
        else:
            status_desc = 'unknown'
        status = f'{status} ({status_desc})'
        return f'<ReadyForQuery status="{status}">'


class RowDescription(PgMessage, side='backend'):
    _type = b'T'
    nfields = Int16
    # after this, for each row field a number of fields follow, parsed
    # in the custom _deserialize method

    def __init__(self, fields=[]):
        self.fields = []

    def __repr__(self):
        return f'<RowDescription {self.fields}>'

    @classmethod
    def _deserialize(cls, msg, start, length):
        obj = cls()
        idx = start
        obj.nfields, n = Int16.deserialize(msg, idx)
        idx += n
        while len(obj.fields) < obj.nfields:
            name, n = String.deserialize(msg, idx)
            idx += n
            table_oid, n = Int32.deserialize(msg, idx)
            idx += n
            column_attr_id, n = Int16.deserialize(msg, idx)
            idx += n
            type_oid, n = Int32.deserialize(msg, idx)
            idx += n
            type_len, n = Int16.deserialize(msg, idx)
            idx += n
            type_modifier, n = Int32.deserialize(msg, idx)
            idx += n
            format_code, n = Int16.deserialize(msg, idx)
            idx += n
            obj.fields.append((name, table_oid, column_attr_id,
                               type_oid, type_len, type_modifier,
                               format_code))
        return obj


class SSLRequest(PgMessage, side='frontend'):
    _type = None  # SSLRequest message has no type field
    ssl_request_code = Int32(80877103)


class StartupMessage(PgMessage, side='frontend'):
    _type = None  # startup message has no type field
    version = Int32(0x0003_0000)  # protocol version 3.0
    # params = dynamic field

    def __init__(self, user, database):
        self.user = user.encode('utf-8')
        self.database = database.encode('utf-8')

    def __repr__(self):
        user = self.user.decode('utf-8')
        database = self.database.decode('utf-8')
        return f'<StartupMessage user={user} db={database}>'

    def params(self):
        return (
            b'user\0' +
            self.user + b'\0' +
            b'database\0' +
            self.database + b'\0' +
            b'datestyle\0iso\0' +
            b'intervalstyle\0iso_8601\0' +
            b'\0'
        )


class Sync(PgMessage, side='frontend'):
    _type = b'S'

    def __repr__(self):
        return '<Sync>'


class Terminate(PgMessage, side='frontend'):
    _type = b'X'

    def __repr__(self):
        return '<Terminate>'

import hashlib

#
# postgres message data types as defined here:
# https://www.postgresql.org/docs/current/protocol-message-types.html
#

class PgDataType:
    pass


class Int32(PgDataType):
    def __init__(self, value: int):
        self.value = value

    def __bytes__(self):
        return self.value.to_bytes(length=4,
                                   byteorder='big',
                                   signed=True)

    def __repr__(self):
        return str(self.value)

    @classmethod
    def deserialize(cls, msg, start):
        value = msg[start:start+4]
        value = int.from_bytes(value, byteorder='big', signed=True)
        return cls(value), 4


class Byte1(PgDataType):
    def __init__(self, value: bytes):
        assert len(value) == 1
        self.value = value

    def __bytes__(self):
        return self.value

    def __repr__(self):
        return f'{self.value} ({self.value[0]})'

    @classmethod
    def deserialize(cls, msg, start):
        return cls(msg[start:start+1]), 1


class String(PgDataType):
    def __init__(self, value: bytes):
        if not isinstance(value, bytes):
            raise ValueError
        self.value = value

    def __bytes__(self):
        return self.value + b'\0'

    def __repr__(self):
        return f'"{str(self)}"'

    def __str__(self):
        try:
            value = self.value.decode('utf-8')
        except UnicodeDecodeError:
            value = self.value
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


# maps message types to their relevant sub-class of PgMessage. this
# will be populated by PgMessageMetaClass.
msg_classes = {}


# metaclass for all postgres message classes, applied through the base
# class PgMessage
class PgMessageMetaClass(type):
    def __new__(cls, name, bases, attrs, **kwargs):
        if name != 'PgMessage':
            if '_type' not in attrs:
                raise TypeError(
                    'PgMessage sub-class should have a _type attribute')

            if attrs['_type'] is not None:
                if not isinstance(attrs['_type'], bytes) or \
                   len(attrs['_type']) != 1:
                    raise ValueError(
                        '_type field should contain a bytes object of '
                        'size 1.')

            for attr, value in attrs.items():
                if attr.startswith('_'):
                    continue
                if not isinstance(value, PgDataType) and \
                   not (isinstance(value, type) and \
                        issubclass(value, PgDataType)) and \
                   not callable(value):
                    raise TypeError(
                        'PgMessage sub-class fields should either by a '
                        'sub-class of PgDataType, or an instance of '
                        'such a sub-class, or a callable returning '
                        'bytes.')

        klass = super().__new__(cls, name, bases, attrs, **kwargs)
        if name != 'PgMessage' and attrs['_type'] is not None:
            msg_classes[attrs['_type']] = klass

        return klass


# this is the base class for all postgres messages. it adds a
# __bytes__ function that serializes an instance of a sub-class to
# bytes.
#
# see list of messages here:
# https://www.postgresql.org/docs/current/protocol-message-formats.html
class PgMessage(metaclass=PgMessageMetaClass):
    def __bytes__(self):
        # calculate payload
        klass = type(self)
        payload = bytearray()
        for attr, field in vars(klass).items():
            if attr.startswith('_'):
                continue
            if isinstance(field, PgDataType):
                # the field contains a concrete value (like Int32(40),
                # which should always contain the value 40)
                value = bytes(field)
            elif isinstance(field, type) and \
                 issubclass(field, PgDataType):
                # the field contains just a type (like Int32); the
                # field value should have been set in the object
                # itself before serialization attempt.
                value = getattr(self, attr)
                if not isinstance(value, PgDataType):
                    # case it to the relevant PgDataType type
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
        subclass = msg_classes.get(msg_type)
        if subclass is None:
            raise ValueError(f'Unknown message type: {msg_type}')

        msg_len = msg[start + 1:start + 5]
        msg_len = int.from_bytes(msg_len, byteorder='big')

        if msg_len > len(msg) - start - 1:
            # not enough data
            return None, 0

        msg = subclass._deserialize(
            msg,
            start + 1 + 4, # one byte for type, 4 for length
            msg_len - 4 # length consists of the length field itself
                        # but not type
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

            if isinstance(value, PgDataType):
                # the field contains a concrete value (like Int32(40),
                # which should always contain the value 40)
                field_type = type(value)
            elif isinstance(value, type) and \
                 issubclass(value, PgDataType):
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
class Authentication(PgMessage):
    _type = b'R'
    auth = Int32(0)
    # depending on the value of the auth field, more fields might
    # follow; a custom _deserialize function will handle these.

    @classmethod
    def _deserialize(cls, msg, start, length):
        auth, _ = Int32.deserialize(msg, start)
        auth = auth.value
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


class AuthenticationOk(Authentication):
    # set this to None so that it's not automatically deserialized
    # (Authentication class will handle deserialization)
    _type = None
    auth = Int32(0)

    def __repr__(self):
        return f'<AuthenticationOk>'


class AuthenticationKerberosV5(Authentication):
    # set this to None so that it's not automatically deserialized
    # (Authentication class will handle deserialization)
    _type = None
    auth = Int32(2)

    def __repr__(self):
        return f'<AuthenticationKerberosV5>'


class AuthenticationCleartextPassword(Authentication):
    # set this to None so that it's not automatically deserialized
    # (Authentication class will handle deserialization)
    _type = None
    auth = Int32(3)

    def __repr__(self):
        return f'<AuthenticationCleartextPassword>'


class AuthenticationMD5Password(Authentication):
    # set this to None so that it's not automatically deserialized
    # (Authentication class will handle deserialization)
    _type = None
    auth = Int32(5)

    def __init__(self, salt):
        self.salt = salt

    def __repr__(self):
        salt = repr(self.salt)[2:-1] # string b prefix and quotes
        return f'<AuthenticationMD5Password salt="{salt}">'


class AuthenticationSCMCredential(Authentication):
    # set this to None so that it's not automatically deserialized
    # (Authentication class will handle deserialization)
    _type = None
    auth = Int32(6)

    def __repr__(self):
        return f'<AuthenticationSCMCredential>'


class AuthenticationGSS(Authentication):
    # set this to None so that it's not automatically deserialized
    # (Authentication class will handle deserialization)
    _type = None
    auth = Int32(7)

    def __repr__(self):
        return f'<AuthenticationGSS>'


class AuthenticationGSSContinue(Authentication):
    # set this to None so that it's not automatically deserialized
    # (Authentication class will handle deserialization)
    _type = None
    auth = Int32(8)

    def __init__(self, auth_data):
        self.auth_data = auth_data

    def __repr__(self):
        auth_data = repr(self.auth_data)[2:-1] # string b prefix and
                                               # quotes
        return f'<AuthenticationGSSContinue auth_data={auth_data}>'


class AuthenticationSSPI(Authentication):
    # set this to None so that it's not automatically deserialized
    # (Authentication class will handle deserialization)
    _type = None
    auth = Int32(9)

    def __repr__(self):
        return f'<AuthenticationSSPI>'


class AuthenticationSASL(Authentication):
    # set this to None so that it's not automatically deserialized
    # (Authentication class will handle deserialization)
    _type = None
    auth = Int32(10)

    def __init__(self, auth_mechanism):
        self.auth_mechanism = auth_mechanism

    def __repr__(self):
        auth_mech = repr(self.auth_mechanism)[2:-1] # string b prefix
                                                    # and quotes
        return f'<AuthenticationSASL auth_mechanism={auth_mech}>'


class AuthenticationSASLContinue(Authentication):
    # set this to None so that it's not automatically deserialized
    # (Authentication class will handle deserialization)
    _type = None
    auth = Int32(11)

    def __init__(self, data):
        self.data = data

    def __repr__(self):
        data = repr(self.data)[2:-1] # string b prefix and quotes
        return f'<AuthenticationSASLContinue auth_mechanism={data}>'


class AuthenticationSASLFinal(Authentication):
    # set this to None so that it's not automatically deserialized
    # (Authentication class will handle deserialization)
    _type = None
    auth = Int32(12)

    def __init__(self, data):
        self.data = data

    def __repr__(self):
        data = repr(self.data)[2:-1] # string b prefix and quotes
        return f'<AuthenticationSASLFinal auth_mechanism={data}>'


class BackendKeyData(PgMessage):
    _type = b'K'
    pid = Int32
    secret_key = Int32

    def __repr__(self):
        return (
            f'<BackendKeyData pid={self.pid} '
            f'secret_key={self.secret_key}>'
        )


class ErrorResponse(PgMessage):
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
        consumed = 0
        pairs = []
        idx = start
        while True:
            if idx >= len(msg):
                raise ValueError(
                    'Unterminated ErrorResponse message (should end '
                    'with a zero byte)')
            code, n = Byte1.deserialize(msg, idx)
            if code.value == b'\0':
                break
            code = repr(code.value)[2:-1] # convert e.g. b'C' to C
            idx += n
            value, n = String.deserialize(msg, idx)
            idx += n
            pairs.append((code, value))
        obj = cls(pairs)
        return obj


class NegotiateProtocolVersion(PgMessage):
    _type = b'v'
    minor_ver_supported = Int32
    n_unrecognized_proto_options = Int32
    option_name = String


class NoticeResponse(PgMessage):
    _type = b'N'
    # the message body contains one or more of (Byte1, String) pairs
    # each denoting a code and a value. The custom _deserializer
    # gathers these in a field named "notices".

    def __init__(self, notices=[]):
        self.notices = []

    def __repr__(self):
        nfields = f'({len(self.notices)} field(s))'
        notices = ' '.join(
            f'{code}={value}' for code, value in self.notices)
        return f'<NoticeMessage {nfields} {notices}>'

    @classmethod
    def _deserialize(cls, msg, start, length):
        idx = start
        while True:
            if idx >= len(msg):
                raise ValueError(
                    'Unterminated NoticeMessage message (should end '
                    'with a zero byte)')
            code, n = Byte1.deserialize(msg, start)
            if code.value == b'\0':
                break
            idx += n
            value, n = String.deserialize(msg, start)
            obj.notices.append((code, value))
            idx += n


class ParameterStatus(PgMessage):
    _type = b'S'
    param_name = String
    param_value = String

    def __repr__(self):
        return (
            f'<ParameterStatus name={self.param_name} '
            f'value={self.param_value}>'
        )


class PasswordMessage(PgMessage):
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
            #password = String(b'md5' + final_hash)
            password = b'md5' + final_hash

        self.password = password

    def __repr__(self):
        password = repr(bytes(self.password))[2:-1]
        return f'<PasswordMessage password="{password}">'


class ReadyForQuery(PgMessage):
    _type = b'Z'
    transaction_status = Byte1

    def __repr__(self):
        try:
            status = self.transaction_status.value.decode('utf-8')
        except UnicodeDecodeError:
            status = str(self.transaction_status.value)

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


class SSLRequest(PgMessage):
    _type = None # SSLRequest message has no type field
    ssl_request_code = Int32(80877103)


class StartupMessage(PgMessage):
    _type = None # startup message has no type field
    version = Int32(0x0003_0000) # protocol version 3.0
    # params = dynamic field

    def __init__(self, user, database):
        self.user = user.encode('utf-8')
        self.database = database.encode('utf-8')

    def params(self):
        return (
            b'user\0' +
            self.user + b'\0' +
            b'database\0' +
            self.database + b'\0' +
            b'\0'
        )

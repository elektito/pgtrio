import warnings
import struct
import socket
import re
import dateutil.parser
import orjson
from datetime import date, time, datetime, timedelta, timezone
from ipaddress import (
    ip_address, ip_network, IPv4Address, IPv6Address, IPv4Network,
    IPv6Network,
)
from functools import wraps
from ._exceptions import InterfaceError
from ._utils import PgProtocolFormat

interval_re = re.compile(
    r'^(P((?P<years>[-\.\d]+?)Y)?((?P<months>[-\.\d]+?)M)?((?P<weeks>[-\.\d]+?)W)?((?P<days>[-\.\d]+?)D)?)?'
    r'(T((?P<hours>[-\.\d]+?)H)?((?P<minutes>[-\.\d]+?)M)?((?P<seconds>[-\.\d]+?)S)?)?$'
)
pg_epoch = datetime(2000, 1, 1)
pg_epoch_utc = datetime(2000, 1, 1, tzinfo=timezone.utc)

builtin_codecs = {}


def register_builtin_codec(codec):
    builtin_codecs[codec.pg_type] = codec
    return codec


class CodecHelper:
    def __init__(self):
        self.initialized = False

        self._codecs = builtin_codecs
        self._oid_to_name = {}
        self._type_name_to_codec = {}

    def init(self, pg_types):
        for name, oid in pg_types:
            # values passed to this message are un-decoded and in text
            # format (because we haven't initialized codecs yet).
            name = name.decode('ascii')
            oid = int(oid.decode('ascii'))
            if name in self._codecs:
                self.enable_codec(name, oid)
        self.initialized = True

    def enable_codec(self, type_name, type_oid):
        self._oid_to_name[type_oid] = type_name

        for name, codec in self._codecs.items():
            if name == type_name:
                self._type_name_to_codec[name] = codec
                break

    def register_codec(self, codec):
        if not isinstance(codec, type) or not issubclass(codec, Codec):
            raise TypeError(
                'Codec should be a sub-class of Codec class')
        self._codecs[codec.pg_type] = codec
        self._type_name_to_codec[codec.pg_type] = codec

    def encode_value(self, value, type_oid, protocol_format):
        if value is None:
            return None

        type_name = self._oid_to_name.get(type_oid)
        if type_name is None:
            raise InterfaceError(f'Uknown type OID: {type_oid}')

        codec = self._type_name_to_codec.get(type_name)
        if type_name is None:
            raise InterfaceError(
                f'No codec to encode parameter "{value}" to type '
                f'"{type_name}"')

        if protocol_format == PgProtocolFormat.TEXT:
            return codec.encode_text(value)
        else:
            return codec.encode_binary(value)

    def decode_row(self, columns, row_desc):
        row = []
        for col, col_desc in zip(columns, row_desc):
            row.append(self.decode_col(col, col_desc))
        return tuple(row)

    def decode_col(self, col, col_desc):
        if col is None:
            return None

        (
            name, table_oid, column_attr_id, type_oid, type_len,
            type_modifier, format_code
        ) = col_desc

        type_name = self._oid_to_name.get(type_oid)
        if type_name is None:
            warnings.warn(
                f'Unknown column OID: {type_oid}; returning un-decoded '
                'data.')
            return col

        codec = self._type_name_to_codec.get(type_name)
        if codec is None:
            warnings.warn(
                f'Unknown column type: {type_name}; returning '
                'un-decoded data.')
            return col

        if format_code == 0:
            return codec.decode_text(col)
        elif format_code == 1:
            return codec.decode_binary(col)
        else:
            raise InterfaceError(
                f'Unexpected column format code: {format_code}')


class CodecMetaclass(type):
    def __new__(cls, name, bases, attrs, **kwargs):
        klass = super().__new__(cls, name, bases, attrs)
        if name == 'Codec' or name.startswith('_'):
            return klass

        if not hasattr(klass, 'pg_type'):
            raise TypeError('Codec class does not have a pg_type field')

        if not isinstance(klass.pg_type, str):
            raise TypeError(
                f'pg_type field must be a string; Got: '
                f'{klass.pg_type!r}')

        if hasattr(klass, 'python_types'):
            if isinstance(klass.python_types, type):
                # convert single values to tuples
                klass.python_types = (klass.python_types,)
            if isinstance(klass.python_types, list):
                klass.python_types = tuple(klass.python_types)
            if not isinstance(klass.python_types, tuple):
                raise TypeError(
                    f'Codec class python_types field should either be '
                    f'a tuple or a type; Got "{klass.python_types}"')

        expected_methods = [
            'decode_text',
            'decode_binary',
            'encode_text',
            'encode_binary',
        ]
        for method_name in expected_methods:
            if not hasattr(klass, method_name):
                raise TypeError(
                    f'Method {method_name} not found in codec class '
                    f'{name}')
            from inspect import getattr_static
            method = getattr_static(klass, method_name)
            if not isinstance(method, classmethod) and \
               not isinstance(method, staticmethod):
                raise TypeError(
                    'Codec encode/decode methods should either be '
                    'class methods or static methods.')

            # add type checkers to encode methods
            if klass.python_types:
                setattr(klass, method_name,
                        cls.get_typechecked_method(
                            klass,
                            getattr(klass, method_name),
                            klass.python_types,
                            klass.pg_type))

        return klass

    @classmethod
    def get_typechecked_method(cls, codec_class, method, types,
                               pg_type_name):
        @wraps(method)
        def encode_wrapper(value, *args, **kwargs):
            if not isinstance(value, types):
                if len(types) == 1:
                    raise TypeError(
                        f'Codec {codec_class.__name__} expects a value '
                        f'of python type "{types[0].__name__}" for '
                        f'postgres type "{pg_type_name}"; Got: '
                        f'{value!r}')
                else:
                    types_str = f'[{", ".join(t.__name__ for t in types)}]'
                    raise TypeError(
                        f'Codec {codec_class.__name__} expects a value '
                        f'of a python type in {types_str} for postgres '
                        f'type "{pg_type_name}"; Got: {value!r}')

            result = method(value, *args, **kwargs)
            if not isinstance(result, bytes):
                raise TypeError(
                    f'Codec {codec_class.__name__} {method.__name__} '
                    f'method did not return a bytes value; Got: '
                    f'{result!r}')
            return result

        @wraps(method)
        def decode_wrapper(value, *args, **kwargs):
            if not isinstance(value, bytes):
                raise TypeError(
                    f'{method.__name__} expects a bytes value')
            result = method(value, *args, **kwargs)
            if method.__name__.startswith('decode_'):
                if not isinstance(result, types):
                    expected_types = \
                        f'[{", ".join(t.__name__ for t in types)}]'
                    raise TypeError(
                        f'Codec {codec_class.__name__} returned an '
                        f'invalid decoded value; Expected types: '
                        f'{expected_types}, Got: {result!r}')
            return result

        if method.__name__.startswith('decode_'):
            return decode_wrapper
        else:
            return encode_wrapper


class Codec(metaclass=CodecMetaclass):
    pass


class _Int(Codec):
    @classmethod
    def decode_text(cls, value):
        return int(value.decode('ascii'))

    @classmethod
    def decode_binary(cls, value):
        return int.from_bytes(value, byteorder='big', signed=True)

    @classmethod
    def encode_text(cls, value):
        return str(value).encode('ascii')

    @classmethod
    def encode_binary(cls, value):
        return value.to_bytes(length=cls._int_length,
                              byteorder='big',
                              signed=True)


@register_builtin_codec
class Int2(_Int):
    pg_type = 'int2'
    python_types = [int]
    _int_length = 2


@register_builtin_codec
class Int4(_Int):
    pg_type = 'int4'
    python_types = [int]
    _int_length = 4


@register_builtin_codec
class Int8(_Int):
    pg_type = 'int8'
    python_types = [int]
    _int_length = 8


@register_builtin_codec
class Bool(Codec):
    pg_type = 'bool'
    python_types = [bool]

    @classmethod
    def decode_text(cls, value):
        return value == b't'

    @classmethod
    def decode_binary(cls, value):
        return value[0] == 1

    @classmethod
    def encode_text(cls, value):
        return b't' if value else b'f'

    @classmethod
    def encode_binary(cls, value):
        return b'\x01' if value else b'\x00'


@register_builtin_codec
class ByteA(Codec):
    pg_type = 'bytea'
    python_types = [bytes]

    @classmethod
    def decode_text(cls, value):
        assert value.startswith(b'\\x')
        return bytes.fromhex(value[2:].decode('ascii'))

    @classmethod
    def decode_binary(cls, value):
        return value

    @classmethod
    def encode_text(cls, value):
        return b'\\x' + value.hex().encode('ascii')

    @classmethod
    def encode_binary(cls, value):
        return value


@register_builtin_codec
class Text(Codec):
    pg_type = 'text'
    python_types = [str]

    @classmethod
    def decode_text(cls, value):
        return value.decode('utf-8')

    @classmethod
    def decode_binary(cls, value):
        return value.decode('utf-8')

    @classmethod
    def encode_text(cls, value):
        return value.encode('utf-8')

    @classmethod
    def encode_binary(cls, value):
        return value.encode('utf-8')


@register_builtin_codec
class Json(Codec):
    pg_type = 'json'
    python_types = [dict, list]

    @classmethod
    def decode_text(cls, value):
        return orjson.loads(value)

    @classmethod
    def decode_binary(cls, value):
        return orjson.loads(value)

    @classmethod
    def encode_text(cls, value):
        return orjson.dumps(value)

    @classmethod
    def encode_binary(cls, value):
        return orjson.dumps(value)


@register_builtin_codec
class Jsonb(Codec):
    pg_type = 'jsonb'
    python_types = [dict, list]

    @classmethod
    def decode_text(cls, value):
        return orjson.loads(value)

    @classmethod
    def decode_binary(cls, value):
        # create memory view so that we can slice without copying
        m = memoryview(value)
        version = m[0]
        if version != 1:
            raise InterfaceError(f'Unexpected JSONB format: {version}')
        return orjson.loads(m[1:])

    @classmethod
    def encode_text(cls, value):
        return orjson.dumps(value)

    @classmethod
    def encode_binary(cls, value):
        return b'\x01' + orjson.dumps(value)


@register_builtin_codec
class Float4(Codec):
    pg_type = 'float4'
    python_types = [float, int]

    @classmethod
    def decode_text(cls, value):
        return float(value.decode('ascii'))

    @classmethod
    def decode_binary(cls, value):
        value, = struct.unpack('!f', value)
        return value

    @classmethod
    def encode_text(cls, value):
        return str(value).encode('ascii')

    @classmethod
    def encode_binary(cls, value):
        return struct.pack('!f', value)


@register_builtin_codec
class Float8(Codec):
    pg_type = 'float8'
    python_types = [float, int]

    @classmethod
    def decode_text(cls, value):
        return float(value.decode('ascii'))

    @classmethod
    def decode_binary(cls, value):
        value, = struct.unpack('!d', value)
        return value

    @classmethod
    def encode_text(cls, value):
        return str(value).encode('ascii')

    @classmethod
    def encode_binary(cls, value):
        return struct.pack('!d', value)


@register_builtin_codec
class Inet(Codec):
    pg_type = 'inet'
    python_types = [IPv4Address, IPv6Address]

    @classmethod
    def decode_text(cls, value):
        return ip_address(value.decode('ascii'))

    @classmethod
    def decode_binary(cls, value):
        return decode_inet_or_cidr(value)

    @classmethod
    def encode_text(cls, value):
        return str(value).encode('ascii')

    @classmethod
    def encode_binary(cls, value):
        return encode_inet_or_cidr(value)


@register_builtin_codec
class Cidr(Codec):
    pg_type = 'cidr'
    python_types = [IPv4Network, IPv6Network]

    @classmethod
    def decode_text(cls, value):
        return ip_network(value.decode('ascii'))

    @classmethod
    def decode_binary(cls, value):
        return decode_inet_or_cidr(value)

    @classmethod
    def encode_text(cls, value):
        return str(value).encode('ascii')

    @classmethod
    def encode_binary(cls, value):
        return encode_inet_or_cidr(value)


@register_builtin_codec
class Char(Codec):
    pg_type = 'bpchar'
    python_types = [str]

    @classmethod
    def decode_text(cls, value):
        return value.decode('utf-8')

    @classmethod
    def decode_binary(cls, value):
        return value.decode('utf-8')

    @classmethod
    def encode_text(cls, value):
        return value[0].encode('utf-8')

    @classmethod
    def encode_binary(cls, value):
        return value[0].encode('utf-8')


@register_builtin_codec
class Varchar(Codec):
    pg_type = 'varchar'
    python_types = [str]

    @classmethod
    def decode_text(cls, value):
        return value.decode('utf-8')

    @classmethod
    def decode_binary(cls, value):
        return value.decode('utf-8')

    @classmethod
    def encode_text(cls, value):
        return value.encode('utf-8')

    @classmethod
    def encode_binary(cls, value):
        return value.encode('utf-8')


@register_builtin_codec
class Date(Codec):
    pg_type = 'date'
    python_types = [date]

    @classmethod
    def decode_text(cls, value):
        return dateutil.parser.parse(value.decode('ascii')).date()

    @classmethod
    def decode_binary(cls, value):
        value = int.from_bytes(value, byteorder='big', signed=True)
        return (pg_epoch + timedelta(days=value)).date()

    @classmethod
    def encode_text(cls, value):
        return value.isoformat().encode('ascii')

    @classmethod
    def encode_binary(cls, value):
        # turn it into a datetime so we can subtract epoch from it
        value = datetime.combine(value, time(0, 0, 0))

        days = (value - pg_epoch).days
        return days.to_bytes(length=4, byteorder='big', signed=True)


@register_builtin_codec
class Time(Codec):
    pg_type = 'time'
    python_types = [time]

    @classmethod
    def decode_text(cls, value):
        return dateutil.parser.parse(value.decode('ascii')).time()

    @classmethod
    def decode_binary(cls, value):
        value = int.from_bytes(value, byteorder='big', signed=True)

        # the epoch value doesn't really matter here, since we strip
        # off the date part
        return (pg_epoch + timedelta(microseconds=value)).time()

    @classmethod
    def encode_text(cls, value):
        return value.isoformat().encode('ascii')

    @classmethod
    def encode_binary(cls, value):
        useconds = (
            value.hour * 3600 * 1_000_000 +
            value.minute * 60 * 1_000_000 +
            value.second * 1_000_000 +
            value.microsecond
        )
        return useconds.to_bytes(length=8, byteorder='big', signed=True)


@register_builtin_codec
class DateTime(Codec):
    pg_type = 'timestamp'
    python_types = [datetime]

    @classmethod
    def decode_text(cls, value):
        return dateutil.parser.isoparse(value.decode('ascii'))

    @classmethod
    def decode_binary(cls, value):
        value = int.from_bytes(value, byteorder='big', signed=True)
        return pg_epoch + timedelta(microseconds=value)

    @classmethod
    def encode_text(cls, value):
        return value.isoformat().encode('ascii')

    @classmethod
    def encode_binary(cls, value):
        useconds = int((value - pg_epoch).total_seconds() * 1_000_000)
        return useconds.to_bytes(length=8, byteorder='big', signed=True)


@register_builtin_codec
class DateTimeTz(Codec):
    pg_type = 'timestamptz'
    python_types = [datetime]

    @classmethod
    def decode_text(cls, value):
        return dateutil.parser.isoparse(value.decode('ascii'))

    @classmethod
    def decode_binary(cls, value):
        value = int.from_bytes(value, byteorder='big', signed=True)
        dt = timedelta(microseconds=value)
        return (pg_epoch_utc + dt).astimezone()

    @classmethod
    def encode_text(cls, value):
        return value.isoformat().encode('ascii')

    @classmethod
    def encode_binary(cls, value):
        seconds = (value - pg_epoch_utc).total_seconds()
        useconds = int(seconds * 1_000_000)
        return useconds.to_bytes(length=8, byteorder='big', signed=True)


@register_builtin_codec
class Interval(Codec):
    pg_type = 'interval'
    python_types = [timedelta]

    @classmethod
    def decode_text(cls, value):
        value = value.decode('ascii')

        m = interval_re.match(value)
        groups = m.groupdict()

        values = {}
        for i in ['years', 'months', 'weeks', 'days', 'hours',
                  'minutes', 'seconds']:
            values[i] = float(groups.get(i) or 0)

        days = values['years'] * 365 + values['months'] * 30 + values['days']
        del values['years']
        del values['months']
        del values['days']

        return timedelta(days=days, **values)

    @classmethod
    def decode_binary(cls, value):
        # see the interval_recv function in
        # src/backend/utils/adt/timestamp.c

        time = value[:8]
        days = value[8:12]
        months = value[12:]

        time = int.from_bytes(time, byteorder='big', signed=True)
        days = int.from_bytes(days, byteorder='big', signed=True)
        months = int.from_bytes(months, byteorder='big', signed=True)

        if months < 0:
            years = -(-months // 12)
            months = -(-months % 12)
        else:
            years = months // 12
            months = months % 12

        time = timedelta(microseconds=time)
        time += timedelta(days=days + months * 30 + years * 365)

        return time

    @classmethod
    def encode_text(cls, value):
        # adapted from: https://github.com/RusticiSoftware/TinCanPython
        # (Apache License 2.0)

        seconds = value.total_seconds()
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        days, hours = divmod(hours, 24)
        days, hours, minutes = map(int, (days, hours, minutes))
        seconds = round(seconds, 6)

        ## build date
        date = ''
        if days:
            date = f'{days}D'

        ## build time
        time = 'T'
        # hours
        bigger_exists = date or hours
        if bigger_exists and hours:
            time += f'{hours:02}H'
        # minutes
        bigger_exists = bigger_exists or minutes
        if bigger_exists and minutes:
          time += f'{minutes:02}M'
        # seconds
        if seconds.is_integer():
            seconds = f'{int(seconds):02}'
        else:
            # 9 chars long w/leading 0, 6 digits after decimal
            seconds = f'{seconds:09.6f}'
        # remove trailing zeros
        seconds = seconds.rstrip('0')
        if seconds:
            time += f'{seconds}S'

        iso_duration = 'P' + date + time
        return iso_duration.encode('ascii')

    @classmethod
    def encode_binary(cls, value):
        useconds = value.microseconds
        useconds += value.seconds * 1_000_000

        # try not to use months if we can (because we'd lose accuracy
        # when we assume a month is always 30 days)
        days = value.days
        if days >= 2**32:
            months = days // 30
            days -= months * 30
        else:
            months = 0

        time = useconds.to_bytes(length=8, byteorder='big', signed=True)
        days = days.to_bytes(length=4, byteorder='big', signed=True)
        months = months.to_bytes(length=4, byteorder='big', signed=True)

        return time + days + months


@register_builtin_codec
class TimeTz(Codec):
    pg_type = 'timetz'
    python_types = [time]

    @classmethod
    def decode_text(cls, value):
        return dateutil.parser.parse(value.decode('ascii')).timetz()

    @classmethod
    def decode_binary(cls, value):
        # see the timetz_recv function in src/backend/utils/adt/date.c
        # in postgresql source code

        timeval = value[:8]
        zone = value[8:]

        useconds = int.from_bytes(timeval, byteorder='big', signed=True)
        zone = int.from_bytes(zone, byteorder='big', signed=True)

        hours = useconds // 3_600_000_000
        useconds -= hours * 3_600_000_000
        minutes = useconds // 60_000_000
        useconds -= minutes * 60_000_000
        seconds = useconds // 1_000_000
        useconds -= seconds * 1_000_000

        # Finally found out the reason for the following line in asyncpg
        # source code (in asyncpg/pgproto/codecs/datetime.pyz, in the
        # timetz_encode function [don't forget to get git submodules in
        # the asyncpg repo to get that file]): "In Python utcoffset() is
        # the difference between the local time and the UTC, whereas in
        # PostgreSQL it's the opposite, so we need to flip the sign."
        zone = -zone

        zone = timezone(timedelta(seconds=zone))

        return time(hours, minutes, seconds, useconds, tzinfo=zone)

    @classmethod
    def encode_text(cls, value):
        return value.isoformat().encode('ascii')

    @classmethod
    def encode_binary(cls, value):
        # see the timetz_send function in src/backend/utils/adt/date.c
        # in postgresql source code

        if value.utcoffset() is None:
            raise ValueError('No tzinfo in the passed time object')

        # for negation reason see decode_binary method
        zone = -int(value.utcoffset().total_seconds())

        useconds = (
            value.hour * 3_600_000_000 +
            value.minute * 60_000_000 +
            value.second * 1_000_000 +
            value.microsecond
        )

        zone = zone.to_bytes(length=4,
                             byteorder='big',
                             signed=True)
        useconds = useconds.to_bytes(length=8,
                                     byteorder='big',
                                     signed=True)

        return useconds + zone


## utils

def decode_inet_or_cidr(value):
    # see thet network_recv function in
    # src/backend/utils/adt/network.c in postgresql source code
    #
    # from the function docs:
    #
    # The external representation is (one byte apiece for) family,
    # bits, is_cidr, address length, address in network byte
    # order. Presence of is_cidr is largely for historical
    # reasons...We send it correctly on output, but ignore the value
    # on input.

    family = value[0]
    bits = value[1]
    is_cidr = value[2]
    address_length = value[3]
    address = value[4:]

    # these are defined in src/include/utils/inet.h. notice that
    # PGSQL_AF_INET6 value is not the same as socket.AF_INET6
    PGSQL_AF_INET = socket.AF_INET + 0
    PGSQL_AF_INET6 = socket.AF_INET + 1

    if family == PGSQL_AF_INET:
        address_class = IPv4Address
        network_class = IPv4Network
        max_prefixlen = 32
        ip_version = 4
    elif family == PGSQL_AF_INET6:
        address_class = IPv6Address
        network_class = IPv6Network
        max_prefixlen = 128
        ip_version = 6
    else:
        raise InterfaceError(
            'Invalid address family code received: {family}')

    if bits > max_prefixlen:
        raise InterfaceError(
            f'Invalid IPv{ip_version} CIDR netmask received: /{bits}')

    if len(address) != address_length:
        raise InterfaceError(
            'Invalid network address: address_length does not match '
            'length of data')

    if bits == max_prefixlen:
        return address_class(address)
    else:
        return network_class((address, bits))


def encode_inet_or_cidr(value):
    # see decode_binary comments

    # these are defined in src/include/utils/inet.h. notice that
    # PGSQL_AF_INET6 value is not the same as socket.AF_INET6
    PGSQL_AF_INET = socket.AF_INET + 0
    PGSQL_AF_INET6 = socket.AF_INET + 1

    if isinstance(value, (IPv4Address, IPv4Network)):
        family = PGSQL_AF_INET
    else:
        family = PGSQL_AF_INET6

    if isinstance(value, IPv4Address):
        bits = 32
        address_length = 4
    elif isinstance(value, IPv6Address):
        bits = 128
        address_length = 16
    elif isinstance(value, IPv4Network):
        bits = value.prefixlen
        address_length = 4
    else:
        bits = value.prefixlen
        address_length = 16

    is_cidr = 1 if isinstance(value, (IPv4Network, IPv6Network)) else 0
    header = struct.pack('!BBBB', family, bits, is_cidr, address_length)

    if isinstance(value, (IPv4Network, IPv6Network)):
        return header + value.network_address.packed
    else:
        return header + value.packed

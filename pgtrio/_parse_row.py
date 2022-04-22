import logging
import math
import json
import re
import socket
import struct
import dateutil.parser
from ipaddress import (
    ip_address, IPv4Address, IPv6Address, IPv4Network, IPv6Network
)
from datetime import date, time, datetime, timedelta, timezone
from ._exceptions import InterfaceError

logger = logging.getLogger(__name__)
interval_re = re.compile(
    r'^(P((?P<years>[-\.\d]+?)Y)?((?P<months>[-\.\d]+?)M)?((?P<weeks>[-\.\d]+?)W)?((?P<days>[-\.\d]+?)D)?)?'
    r'(T((?P<hours>[-\.\d]+?)H)?((?P<minutes>[-\.\d]+?)M)?((?P<seconds>[-\.\d]+?)S)?)?$'
)
pg_epoch = datetime(2000, 1, 1)
pg_epoch_utc = datetime(2000, 1, 1, tzinfo=timezone.utc)


def parse(row, row_desc):
    parsed_row = []
    for col, col_desc in zip(row, row_desc):
        parsed_row.append(parse_col(col, col_desc))
    return tuple(parsed_row)


def parse_col(col, col_desc):
    if col is None:
        return None

    (
        name, table_oid, column_attr_id, type_oid, type_len,
        type_modifier, format_code
    ) = col_desc

    # the following oids can be obtained by querying the pg_type
    # table, or from the src/include/catalog/pg_type.dat file in
    # postgres source code

    text_parse_funcs = {
        16: text_parse_bool,
        17: text_parse_bytea,
        20: text_parse_int8,
        21: text_parse_int2,
        23: text_parse_int4,
        25: text_parse_text,
        114: text_parse_json,
        700: text_parse_float4,
        701: text_parse_float8,
        869: text_parse_inet,
        1042: text_parse_bpchar,
        1043: text_parse_varchar,
        1082: text_parse_date,
        1083: text_parse_time,
        1114: text_parse_timestamp,
        1184: text_parse_timestamptz,
        1186: text_parse_interval,
        1266: text_parse_timetz,
        3802: text_parse_jsonb,
    }

    binary_parse_funcs = {
        16: binary_parse_bool,
        17: binary_parse_bytea,
        20: binary_parse_int8,
        21: binary_parse_int2,
        23: binary_parse_int4,
        25: binary_parse_text,
        114: binary_parse_json,
        700: binary_parse_float4,
        701: binary_parse_float8,
        869: binary_parse_inet,
        1042: binary_parse_bpchar,
        1043: binary_parse_varchar,
        1082: binary_parse_date,
        1083: binary_parse_time,
        1114: binary_parse_timestamp,
        1184: binary_parse_timestamptz,
        1186: binary_parse_interval,
        1266: binary_parse_timetz,
        3802: binary_parse_jsonb,
    }

    if format_code.value == 0:
        parse_func = text_parse_funcs.get(type_oid.value)
    elif format_code.value == 1:
        parse_func = binary_parse_funcs.get(type_oid.value)
    else:
        raise InterfaceError(
            f'Unexpected column format code: {format_code.value}')

    if parse_func is None:
        logger.debug(
            f'Did not find a column parser for type oid: '
            f'{type_oid.value}')
        try:
            return col.decode('utf-8')
        except UnicodeDecodeError:
            return col

    return parse_func(col)


def text_parse_bool(value):
    return value == b't'


def binary_parse_bool(value):
    return value[0] == 1


def text_parse_bytea(value):
    assert value.startswith(b'\\x')
    return bytes.fromhex(value[2:].decode('ascii'))


def binary_parse_bytea(value):
    return value


def text_parse_int8(value):
    return int(value.decode('ascii'))


def binary_parse_int8(value):
    return int.from_bytes(value, byteorder='big', signed=True)


def text_parse_int2(value):
    return int(value.decode('ascii'))


def binary_parse_int2(value):
    return int.from_bytes(value, byteorder='big', signed=True)


def text_parse_int4(value):
    return int(value.decode('ascii'))


def binary_parse_int4(value):
    return int.from_bytes(value, byteorder='big', signed=True)


def text_parse_text(value):
    return value.decode('utf-8')


def binary_parse_text(value):
    return value.decode('utf-8')


def text_parse_json(value):
    return json.loads(value.decode('utf-8'))


def binary_parse_json(value):
    return json.loads(value.decode('utf-8'))


def text_parse_float4(value):
    return float(value.decode('ascii'))


def binary_parse_float4(value):
    value, = struct.unpack('!f', value)
    return value


def text_parse_float8(value):
    return float(value.decode('ascii'))


def binary_parse_float8(value):
    value, = struct.unpack('!d', value)
    return value


def text_parse_inet(value):
    return ip_address(value.decode('ascii'))


def binary_parse_inet(value):
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
        ip_ver = sample_addr.version
        raise InterfaceError(
            'Invalid IPv{ip_version} CIDR netmask received: /{bits}')

    if len(address) != address_length:
        raise InterfaceError(
            'Invalid network address: address_length does not match '
            'length of data')

    if bits == max_prefixlen:
        return address_class(address)
    else:
        return network_class((address, bits))


def text_parse_bpchar(value):
    return value.decode('utf-8')


def binary_parse_bpchar(value):
    return value.decode('utf-8')


def text_parse_varchar(value):
    return value.decode('utf-8')


def binary_parse_varchar(value):
    return value.decode('utf-8')


def text_parse_date(value):
    return dateutil.parser.parse(value.decode('utf-8')).date()


def binary_parse_date(value):
    value = int.from_bytes(value, byteorder='big', signed=True)
    return (pg_epoch + timedelta(days=value)).date()


def text_parse_time(value):
    return dateutil.parser.parse(value.decode('utf-8')).time()


def binary_parse_time(value):
    value = int.from_bytes(value, byteorder='big', signed=True)
    return (pg_epoch + timedelta(microseconds=value)).time()


def text_parse_timestamp(value):
    return dateutil.parser.isoparse(value.decode('utf-8'))


def binary_parse_timestamp(value):
    value = int.from_bytes(value, byteorder='big', signed=True)
    return pg_epoch + timedelta(microseconds=value)


def text_parse_timestamptz(value):
    return dateutil.parser.isoparse(value.decode('utf-8'))


def binary_parse_timestamptz(value):
    value = int.from_bytes(value, byteorder='big', signed=True)
    return (pg_epoch_utc + timedelta(microseconds=value)).astimezone()


def text_parse_interval(value):
    value = value.decode('utf-8')

    m = interval_re.match(value)
    groups = m.groupdict()

    values = {}
    for i in ['years', 'months', 'weeks', 'days', 'hours', 'minutes',
              'seconds']:
        values[i] = float(groups.get(i) or 0)

    days = values['years'] * 365 + values['months'] * 30 + values['days']
    del values['years']
    del values['months']
    del values['days']

    return timedelta(days=days, **values)


def binary_parse_interval(value):
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


def text_parse_timetz(value):
    return dateutil.parser.parse(value.decode('utf-8')).timetz()


def binary_parse_timetz(value):
    # see the timetz_recv function in src/backend/utils/adt/date.c in
    # postgresql source code

    timeval = value[:8]
    zone = value[8:]

    timeval = int.from_bytes(timeval, byteorder='big', signed=True)
    zone = int.from_bytes(zone, byteorder='big', signed=True)

    timeval = timedelta(microseconds=timeval)
    timeval = timeval.total_seconds()

    hours = math.floor(timeval / 3600)
    timeval -= hours * 3600
    minutes = math.floor(timeval / 60)
    timeval -= minutes * 60
    seconds = math.floor(timeval)
    microseconds = int((timeval - seconds) * 1000000)

    # Finally found out the reason for the following line in asyncpg
    # source code (in asyncpg/pgproto/codecs/datetime.pyz, in the
    # timetz_encode function [don't forget to get git submodules in
    # the asyncpg repo to get that file]): "In Python utcoffset() is
    # the difference between the local time and the UTC, whereas in
    # PostgreSQL it's the opposite, so we need to flip the sign."
    zone = -zone

    zone = timezone(timedelta(seconds=zone))

    return time(hours, minutes, seconds, microseconds, tzinfo=zone)


def text_parse_jsonb(value):
    return json.loads(value.decode('utf-8'))


def binary_parse_jsonb(value):
    version = value[0]
    if version != 1:
        raise InterfaceError(f'Unexpected JSONB format: {version}')

    return json.loads(value[1:])

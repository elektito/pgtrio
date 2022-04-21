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
    r'^'
    r'((?P<years>[-+]?\d+) years? ?)?'
    r'((?P<months>[-+]?\d+) mons? ?)?'
    r'((?P<days>[-+]?\d+) days? ?)?'
    r'((?P<time_sign>[-+])?(?P<hours>\d+):(?P<minutes>\d+):(?P<seconds>\d+)(\.(?P<microseconds>\d+))?)?'
    r'$'
)
pg_epoch = datetime(2000, 1, 1)
pg_epoch_utc = datetime(2000, 1, 1, tzinfo=timezone.utc)


class Interval:
    def __init__(self, years=0, months=0, days=0, hours=0, minutes=0,
                 seconds=0, microseconds=0):
        self.years = years
        self.months = months
        self.days = days
        self.hours = hours
        self.minutes = minutes
        self.seconds = seconds
        self.microseconds = microseconds

    def __repr__(self):
        def fmt(value, unit):
            if not value:
                return ''
            return f'{value} {unit}{"s" if value > 1 else ""} '
        desc = ''
        desc += fmt(self.years, 'year')
        desc += fmt(self.months, 'month')
        desc += fmt(self.days, 'day')
        desc += fmt(self.hours, 'hour')
        desc += fmt(self.minutes, 'minute')
        desc += fmt(self.seconds, 'second')
        desc += fmt(self.microseconds, 'microsecond')
        desc = desc.rstrip()
        return f'<Interval {desc}>'


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
    return struct.unpack('!q', value)


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
    return struct.unpack('!f', value)


def text_parse_float8(value):
    return float(value.decode('ascii'))


def binary_parse_float8(value):
    return struct.unpack('!d', value)


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

    years = groups.get('years', 0)
    if years:
        years = int(years)

    months = groups.get('months', 0)
    if months:
        months = int(months)

    days = groups.get('days', 0)
    if days:
        days = int(days)

    sign = groups.get('time_sign')
    sign = -1 if sign == '-' else 1

    hours = groups.get('hours', 0)
    if hours:
        hours = sign * int(hours)

    minutes = groups.get('minutes', 0)
    if minutes:
        minutes = sign * int(minutes)

    seconds = groups.get('seconds', 0)
    if seconds:
        seconds = sign * int(seconds)

    microseconds = groups.get('microseconds', 0)
    if microseconds:
        # make sure the number is six digits so we get the correct
        # microsecond value
        if len(microseconds) < 6:
            microseconds += '0' * (6 - len(microseconds))
        microseconds = sign * int(microseconds)

    years = years or 0
    months = months or 0
    days = days or 0
    hours = hours or 0
    minutes = minutes or 0
    seconds = seconds or 0
    microseconds = microseconds or 0

    # we can't use python's timedelta here because it does not support
    # years and months; and we can't reasonably convert those to days
    # because it would require to know what sort of year and month we
    # have (i.e. how many days there are in them).
    return Interval(
        years = years,
        months=months,
        days=days,
        hours=hours,
        minutes=minutes,
        seconds=seconds,
        microseconds=microseconds
    )


def binary_parse_interval(value):
    # see the interval_recv function in
    # src/backend/utils/adt/timestamp.c

    time = value[:8]
    days = value[8:12]
    months = value[12:]

    time = int.from_bytes(time, byteorder='big', signed=True)
    days = int.from_bytes(days, byteorder='big', signed=True)
    months = int.from_bytes(months, byteorder='big', signed=True)

    time = timedelta(microseconds=time)
    time = time.total_seconds()

    # calculate the hours, minutes, seconds and microseconds with the
    # absolute value of time, then add the sign. This way, we get
    # saner values. For example, if we don't do this, instead of -2
    # seconds, we'd get "-1 hours 59 minutes 58 seconds" which is
    # technically correct, but not one might expect.
    sign = -1 if time < 0 else 1
    time = abs(time)

    hours = math.floor(time / 3600)
    time -= hours * 3600
    minutes = math.floor(time / 60)
    time -= minutes * 60
    seconds = math.floor(time)
    microseconds = int((time - seconds) * 1000000)

    hours = sign * hours
    minutes = sign * minutes
    seconds = sign * seconds
    microseconds = sign * microseconds

    years = math.floor(months / 12)
    months -= years * 12

    return Interval(
        years = years,
        months=months,
        days=days,
        hours=hours,
        minutes=minutes,
        seconds=seconds,
        microseconds=microseconds
    )


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

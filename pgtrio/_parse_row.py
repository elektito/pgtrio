import logging
import json
import re
import dateutil.parser
from ipaddress import ip_address
from datetime import date, time, datetime, timedelta

logger = logging.getLogger(__name__)
interval_re = re.compile(
    r'^'
    r'((?P<years>[-+]?\d+) years? ?)?'
    r'((?P<months>[-+]?\d+) mons? ?)?'
    r'((?P<days>[-+]?\d+) days? ?)?'
    r'((?P<time_sign>[-+])?(?P<hours>\d+):(?P<minutes>\d+):(?P<seconds>\d+)(\.(?P<microseconds>\d+))?)?'
    r'$'
)


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

    parse_func = {
        # the following oids can be obtained by querying the pg_type
        # table, or from the src/include/catalog/pg_type.dat file in
        # postgres source code

        16: parse_bool,
        17: parse_bytea,
        20: parse_int8,
        21: parse_int2,
        23: parse_int4,
        114: parse_json,
        700: parse_float4,
        701: parse_float8,
        869: parse_inet,
        1042: parse_bpchar,
        1043: parse_varchar,
        1082: parse_date,
        1083: parse_time,
        1114: parse_timestamp,
        1184: parse_timestamptz,
        1186: parse_interval,
        1266: parse_timetz,
        3802: parse_jsonb,
    }.get(type_oid.value)

    if parse_func is None:
        logger.debug(
            f'Did not find a column parser for type oid: '
            f'{type_oid.value}')
        return col.decode('utf-8')

    return parse_func(col)


def parse_bool(value):
    return value == b't'


def parse_bytea(value):
    assert value.startswith(b'\\x')
    return bytes.fromhex(value[2:].decode('ascii'))


def parse_int8(value):
    return int(value.decode('ascii'))


def parse_int2(value):
    return int(value.decode('ascii'))


def parse_int4(value):
    return int(value.decode('ascii'))


def parse_json(value):
    return json.loads(value.decode('utf-8'))


def parse_float4(value):
    return float(value.decode('ascii'))


def parse_float8(value):
    return float(value.decode('ascii'))


def parse_inet(value):
    return ip_address(value.decode('ascii'))


def parse_bpchar(value):
    return value.decode('utf-8')


def parse_varchar(value):
    return value.decode('utf-8')


def parse_date(value):
    return dateutil.parser.parse(value.decode('utf-8')).date()


def parse_time(value):
    return dateutil.parser.parse(value.decode('utf-8')).time()


def parse_timestamp(value):
    return dateutil.parser.isoparse(value.decode('utf-8'))


def parse_timestamptz(value):
    return dateutil.parser.isoparse(value.decode('utf-8'))


def parse_interval(value):
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


def parse_timetz(value):
    return dateutil.parser.parse(value.decode('utf-8')).timetz()


def parse_jsonb(value):
    return json.loads(value.decode('utf-8'))

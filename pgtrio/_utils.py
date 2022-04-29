from enum import IntEnum


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

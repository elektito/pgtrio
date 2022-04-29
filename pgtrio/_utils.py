from enum import IntEnum, Enum


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


class PgIsolationLevel(Enum):
    SERIALIZABLE = 1
    REPEATABLE_READ = 2
    READ_COMMITTED = 3
    READ_UNCOMMITTED = 4

    def __str__(self):
        return self.name.lower().replace('_', ' ')


class PgReadWriteMode(Enum):
    READ_WRITE = 1
    READ_ONLY = 2

    def __str__(self):
        return self.name.lower().replace('_', ' ')

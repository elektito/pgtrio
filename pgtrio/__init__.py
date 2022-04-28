from ._dbapi import connect, PgProtocolFormat
from ._codecs import Codec
from ._exceptions import (
    Warning, Error, InterfaceError, DatabaseError, DataError,
    OperationalError, IntegrityError, InternalError, ProgrammingError,
    NotSupportedError,
)

api_level = '2.0'
threadsafety = 0 # threads may not share the module
paramstyle = 'pyformat' # like %(name)s

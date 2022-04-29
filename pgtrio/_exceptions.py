class Warning(Exception):
    pass


class Error(Exception):
    def __init__(self, error_msg):
        self.error_msg = error_msg


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    def __init__(self, error_msg, severity=None):
        self.severity = severity
        self.error_msg = error_msg

    def __str__(self):
        severity = self.severity or 'UNKNOWN-SEVERITY'
        return f'Database error: [{severity}] {self.error_msg}'


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass

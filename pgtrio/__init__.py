from ._dbapi import connect
from ._exceptions import (
    Warning, Error, InterfaceError, DatabaseError, DataError,
    OperationalError, IntegrityError, InternalError, ProgrammingError,
    NotSupportedError,
)

api_level = '2.0'
threadsafety = 0 # threads may not share the module
paramstyle = 'pyformat' # like %(name)s


async def xconnect(unix_socket_path):
    #stream = await trio.open_unix_socket(unix_socket_path)
    stream = await trio.open_tcp_stream('localhost', 5432)

    msg = StartupMessage('mostafa', 'tridht2')
    msg = bytes(msg)

    await stream.send_all(msg)

    resp = await stream.receive_some(1024)
    print(resp)

    start = 0
    while True: # 1 byte msg type, 4 bytes length
        r, length = PgMessage.deserialize(resp, start)
        if isinstance(r, AuthenticationMD5Password):
            salt = r.salt
        if r is None:
            break
        print(r)
        start += length

    rest = resp[start:]
    print('rest:', rest)

    pwd_msg = PasswordMessage('enter', md5=True,
                              username='mostafa', salt=salt)

    msg = bytes(pwd_msg)
    await stream.send_all(msg)

    resp = await stream.receive_some(1024)
    print(resp)

    start = 0
    while True: # 1 byte msg type, 4 bytes length
        r, length = PgMessage.deserialize(resp, start)
        if isinstance(r, AuthenticationMD5Password):
            salt = r.salt
        if r is None:
            break
        print(r)
        start += length
######################## delete above func #######################3


async def main():
    await connect('/var/run/postgresql/.s.PGSQL.5432')


if __name__ == '__main__':
    trio.run(main)


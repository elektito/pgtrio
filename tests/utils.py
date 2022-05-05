import subprocess
import os
import pgtrio
from tempfile import TemporaryDirectory, mkstemp
from pathlib import Path
from pytest import fixture

pg_ctl = None


@fixture
def postgres_socket_file():
    global pg_ctl
    if not pg_ctl:
        pg_ctl = find_pg_ctl()

    with TemporaryDirectory() as data_dir:
        # use the same directory for both data and the unix socket
        socket_dir = data_dir

        _, log_file = mkstemp(suffix='.log')

        init_cmd = [
            pg_ctl,
            'init',
            '-D', data_dir,
            '-l', log_file,
        ]
        proc = subprocess.run(init_cmd, capture_output=True)
        if proc.returncode:
            raise RuntimeError(
                f'Could not initilize a  PostgreSQL data directory '
                f'(exit code={proc.returncode}). Check the log file: '
                f'{log_file}\nstdout:\n{proc.stdout.decode()}\n\n'
                f'stderr:\n{proc.stderr.decode()}')

        pg_options = (
            f'-F -c listen_addresses="" '
            f'-c unix_socket_directories={socket_dir} '
        )
        start_cmd = [
            pg_ctl,
            '-D', data_dir,
            '-l', log_file,
            '-m', 'immediate',
            'start',
            '-o', pg_options,
        ]

        proc = subprocess.run(start_cmd, capture_output=True)
        if proc.returncode:
            raise RuntimeError(
                f'Could not start PostgreSQL (exit code='
                f'{proc.returncode}). Check the log file: {log_file}'
                f'\nstdout:\n{proc.stdout.decode()}\n\n'
                f'stderr:\n{proc.stderr.decode()}')

        yield f'{data_dir}/.s.PGSQL.5432'

        stop_cmd = [
            pg_ctl,
            '-D', data_dir,
            '-l', log_file,
            '-m', 'immediate',
            'stop',
            '-o', pg_options,
        ]

        proc = subprocess.run(stop_cmd, capture_output=True)
        if proc.returncode:
            raise RuntimeError(
                f'Could not stop PostgreSQL (exit code='
                f'{proc.returncode}).\nstdout:\n{proc.stdout.decode()}'
                f'\n\nstderr:\n{proc.stderr.decode()}')

        os.unlink(log_file)


@fixture(params=['binary', 'text'])
async def conn(postgres_socket_file, request):
    fmt = request.param
    async with pgtrio.connect(
            'postgres',
            protocol_format=fmt,
            unix_socket_path=postgres_socket_file) as conn:
        await conn.execute('create database testdb')

    async with pgtrio.connect(
            'testdb',
            unix_socket_path=postgres_socket_file) as conn:

        # owner check won't work in tests, because fixtures are not
        # created in the same task as the test function
        conn._disable_owner_check = True

        yield conn


@fixture(params=['binary', 'text'])
async def pool(postgres_socket_file, request):
    fmt = request.param
    async with pgtrio.connect(
            'postgres',
            protocol_format=fmt,
            unix_socket_path=postgres_socket_file) as conn:
        await conn.execute('create database testdb')

    def conn_init(conn):
        # owner check won't work in tests, because fixtures are not
        # created in the same task as the test function
        conn._disable_owner_check = True

    async with pgtrio.create_pool(
            'testdb',
            pool_conn_init=conn_init,
            unix_socket_path=postgres_socket_file) as pool:
        yield pool


def find_pg_ctl():
    prefix = Path('/usr/lib/postgresql')
    if not prefix.exists():
        raise RuntimeError(
            f'PostgreSQL directory {prefix} does not exist.')

    version_dirs = sorted(
        (float(str(ver_dir.parts[-1])), ver_dir)
        for ver_dir in prefix.iterdir()
        if ver_dir.is_dir()
    )
    if not version_dirs:
        raise RuntimeError(
            f'No version directories found in {prefix}')

    _, highest_version_dir = version_dirs[-1]
    pg_ctl = highest_version_dir / 'bin/pg_ctl'

    if not pg_ctl.exists():
        raise RuntimeError(
            f'pg_ctl not found at the expected location: {pg_ctl}')

    return str(pg_ctl)

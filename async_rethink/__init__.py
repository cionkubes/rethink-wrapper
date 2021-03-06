import rethinkdb as r
import functools
import asyncio
import socket
import sys

from aioreactive.operators import from_async_iterable, merge, from_iterable
from aioreactive.core import AsyncObservable, Operators
from collections import defaultdict
from operator import itemgetter
from logzero import logger
from typing import Callable
from uuid import uuid4

from .reactive import share
from .log import RethinkEventsLogger, EventsFormatter

r.set_loop_type('asyncio')


async def connection(*args, **kwargs):
    conn = Connection(*args, **kwargs)
    await conn.connect()
    return conn


def require_connection(fn):
    @functools.wraps(fn)
    def wrapper(self, *args, **kwargs):
        assert self.conn is not None, "Method requires a connection"
        return fn(self, *args, **kwargs)

    return wrapper


def set_keepalive(sock, after_idle_sec=60*5, interval_sec=3, max_fails=5):
    if sys.platform in ['win32', 'cygwin']:
        sock.ioctl(socket.SIO_KEEPALIVE_VALS,
                   (1, 1000*after_idle_sec, 1000*interval_sec))
    elif sys.platform == 'darwin':
        TCP_KEEPALIVE = 0x10
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.IPPROTO_TCP, TCP_KEEPALIVE, interval_sec)
    elif sys.platform == 'linux':
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.IPPROTO_TCP,
                        socket.TCP_KEEPIDLE, after_idle_sec)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval_sec)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, max_fails)


class Connection:
    db_name = 'cion'

    def __init__(self, addr, port, retry_timeout=3, db_name=db_name):
        self.addr = addr
        self.port = port
        self.conn = None
        self.db_name = db_name
        self.observables = keydefaultdict(self.changefeed_observable)
        self.retry_timeout = retry_timeout

    async def connect(self):
        while True:
            try:
                self.conn = await r.connect(self.addr, self.port)
            except r.ReqlDriverError:
                logger.critical(
                    f"Failed to connect to database, retrying in {self.retry_timeout} seconds.")
                await asyncio.sleep(self.retry_timeout)
            else:
                sock = self.conn._instance._streamwriter.get_extra_info('socket')
                set_keepalive(sock)
                break

    def observe(self, table) -> AsyncObservable:
        return self.observables[table]

    def close(self):
        self.conn.close()

    def db(self):
        return r.db(self.db_name)

    async def run(self, query):
        return await query.run(self.conn)

    def get_log_handler(self, emitter):
        @desyncify
        async def insert(event):
            await self.run(self.db().table('tasks').insert(event))

        handler = RethinkEventsLogger(insert)
        handler.setFormatter(EventsFormatter(emitter))

        return handler

    async def iter(self, query):
        feed = await self.run(query)

        while await feed.fetch_next():
            yield await feed.next()

    async def list(self, query):
        result = []

        async for item in self.iter(query):
            result.append(item)

        return result

    def observable_query(self, query):
        return from_async_iterable(self.iter(query))

    def start_with_and_changes(self, query, unpack=('new_val', 'old_val')):
        return merge(from_iterable([
            self.observable_query(query)\
                | Operators.map(lambda item: {unpack[0]: item, unpack[1]: None}),
            self.observable_query(query.changes())])
        )

    def changes_accumulate(self, query, pk='id', unpack=('new_val', 'old_val')):
        unpacker = itemgetter(*unpack)
        accumulator = {}

        def update(change):
            new, old = unpacker(change)

            if new is None:
                accumulator.pop(old[pk], None)
            elif old is None:
                accumulator[new[pk]] = new
            elif old[pk] != new[pk]:
                accumulator.pop(old[pk], None)
                accumulator[new[pk]] = new
            else:
                accumulator[new[pk]] = new

            return accumulator.values()

        return self.start_with_and_changes(query, unpack=unpack) | Operators.map(update)

    def changefeed_observable(self, table):
        logger.debug(f"Creating observable for {table}")
        def logthrough(name):
            def log(x):
                logger.debug(f"{name}: {x}")
                return x

            return Operators.map(log)

        query = self.db().table(table).changes()
        return self.observable_query(query) | share

class keydefaultdict(defaultdict):
    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)
        else:
            ret = self[key] = self.default_factory(key)
            return ret


def desyncify(fn, loop=asyncio.get_event_loop()):
    tasks = {}
    stopped = False

    async def wait_then_remove(awaitable, uuid):
        await awaitable
        del tasks[uuid]

    def wrapper(*args, **kwargs):
        if stopped:
            raise Exception(f"desyncify {fn} has been stopped.")

        uuid = uuid4()
        tasks[uuid] = loop.create_task(
            wait_then_remove(fn(*args, **kwargs), uuid))

    def shutdown():
        nonlocal stopped
        stopped = True
        return asyncio.gather(tasks.values())

    wrapper.shutdown = shutdown
    return wrapper

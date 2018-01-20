import socket
import sys
import functools
from collections import defaultdict
from contextlib import contextmanager
from typing import Callable

import asyncio
import rethinkdb as r
import rx.concurrency
from logzero import logger
from rx import Observable

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


scheduler = rx.concurrency.AsyncIOScheduler(asyncio.get_event_loop())


class NewConnectionException(Exception):
    pass


def set_keepalive(sock, after_idle_sec=60*5, interval_sec=3, max_fails=5):
    if sys.platform in ['win32', 'cygwin']:
        sock.ioctl(socket.SIO_KEEPALIVE_VALS, (1, 1000*after_idle_sec, 1000*interval_sec))
    elif sys.platform == 'darwin':
        TCP_KEEPALIVE = 0x10
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.IPPROTO_TCP, TCP_KEEPALIVE, interval_sec)
    elif sys.platform == 'linux':
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, after_idle_sec)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval_sec)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, max_fails)


class Connection:
    db_name = 'cion'
    rethink_changefeed_wait = 300

    def __init__(self, addr, port, retry_timeout=3):
        self.addr = addr
        self.port = port
        self.conn = None
        self.observables = keydefaultdict(self.changefeed_observable)
        self.connection_available = asyncio.Event()
        self.retry_timeout = retry_timeout

    async def connect(self):
        while True:
            try:
                self.conn = await r.connect(self.addr, self.port)
                sock = self.conn._instance._streamwriter.get_extra_info('socket')
                print(sock)
                set_keepalive(sock)
                break
            except r.ReqlDriverError:
                logger.critical(f"Failed to connect to database, retrying in {self.retry_timeout} seconds.")
                await asyncio.sleep(self.retry_timeout)

        self.connection_available.set()
        self.connection_available.clear()

    def observe(self, table) -> Observable:
        return self.observables[table]

    def close(self):
        self.conn.close()

    async def reconnect(self):
        self.conn.close()
        await self.connect()

    def db(self):
        return r.db(Connection.db_name)

    def run(self, query):
        return query.run(self.conn)

    async def run_iter(self, query):
        feed = await query.run(self.conn)

        while await feed.fetch_next():
            yield await feed.next()

    @contextmanager
    def changes(self, table):
        feed = None

        async def iterable():
            nonlocal feed
            while True:
                try:
                    feed = await r.db(Connection.db_name).table(table).changes().run(self.conn)

                    while True:
                        yield await unless(
                            feed.next(wait=Connection.rethink_changefeed_wait),
                            self.connection_available.wait(),
                            ex=NewConnectionException
                        )
                except r.ReqlTimeoutError:
                    logger.debug(
                        f"Change feed {table} empty for {Connection.rethink_changefeed_wait} seconds, resetting connection.")

                    if feed is not None:
                        feed.close()

                    await self.reconnect()
                except NewConnectionException:
                    logger.debug(f"Change feed {table} got new connection, reacquiring feed.")

                    if feed is not None:
                        feed.close()

        yield iterable()
        feed.close()

    def changefeed_observable(self, table):
        logger.debug(f"Creating observable for {table}")

        def subscribe(obs):
            logger.debug(f"Subscribed to {table} change feed.")

            async def push_changes():
                with self.changes(table) as changes:
                    async for change in changes:
                        obs.on_next(change)

            task = asyncio.ensure_future(push_changes())

            def done(fut):
                e = fut.exception()
                if e is not None:
                    obs.on_error(e)

                obs.on_completed()

            task.add_done_callback(done)

            def dispose():
                logger.debug(f"Disposed of {table} change feed subscription.")
                task.cancel()

            return dispose

        return Observable.create(subscribe).subscribe_on(scheduler).share()


class keydefaultdict(defaultdict):
    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError( key )
        else:
            ret = self[key] = self.default_factory(key)
            return ret


async def unless(task, other, ex: Callable[[], Exception]=AssertionError):
    task_fut = asyncio.ensure_future(task)
    other_fut = asyncio.ensure_future(other)

    done, pending = await asyncio.wait([task_fut, other_fut], return_when=asyncio.FIRST_COMPLETED)

    if task_fut not in done:
        task_fut.cancel()

        raise ex()

    other_fut.cancel()
    return task_fut.result()

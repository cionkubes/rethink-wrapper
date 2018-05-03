import asyncio
from logzero import logger

from aioreactive.core import AsyncObservable, AsyncDisposable, AsyncObserver, subscribe


def multicast(subject):
    def inner(in_obs):
        return AsyncMulticastObservable(subject, in_obs)

    return inner


def with_latest_from(slave):
    def inner(master):
        return AsyncWithLatestFromObservable(master, slave)

    return inner


def publish(obs):
    return multicast(AsyncSubject())(obs)


def share(obs):
    return publish(obs).ref_count()


class AsyncRefCountObservable(AsyncObservable):
    def __init__(self, source):
        super().__init__()

        self.count = 0
        self.source = source
        self.subscription = None

    async def __asubscribe__(self, observer):
        self.count += 1
        dispose = await subscribe(self.source, observer)

        if self.count == 1:
            self.subscription = await self.source.connect()
        
        async def cancel():
            await dispose.adispose()
            self.count -= 1

            if self.count == 0:
                await self.subscription.adispose()

        return AsyncDisposable(cancel)


class AsyncRepeatedlyCallWithLatest(AsyncObserver):
    def __init__(self, fn):
        self.fn = fn
        self.future = None

    async def call_with(self, value):
        try:
            while True:
                await self.fn(value)
        except asyncio.CancelledError:
            pass

    async def asend(self, value):
        if self.future is not None:
            self.future.cancel()
            await self.future

        self.future = asyncio.ensure_future(self.call_with(value))

    async def athrow(self, ex):
        if self.future is not None:
            self.future.set_exception(ex)

        await self.future

    async def aclose(self):
        if self.future is not None:
            self.future.cancel()

        await self.future


class AsyncInheritObserver(AsyncObserver):
    def __init__(self, observer, send):
        self.send = send
        self.observer = observer

    def asend(self, value):
        return self.send(value)

    def athrow(self, ex):
        return self.observer.athrow(ex)

    def aclose(self):
        return self.observer.aclose()


class AsyncWithLatestFromObservable(AsyncObservable):
    def __init__(self, master, slave):
        super().__init__()
        
        self.master = master
        self.slave = slave

        self.latest_from_slave = None
        self.slave_has_value = asyncio.Event()

    async def __asubscribe__(self, observer):
        async def slave_observer(value):
            self.latest_from_slave = value
            self.slave_has_value.set()

        async def master_observer(value):
            await self.slave_has_value.wait()
            return await observer.asend((value, self.latest_from_slave))

        msub, ssub = await asyncio.gather(
            subscribe(self.master, AsyncInheritObserver(observer, master_observer)),
            subscribe(self.slave, AsyncInheritObserver(observer, slave_observer))
        )

        async def cancel():
            await asyncio.gather(
                msub.adispose(),
                ssub.adispose()
            )

        return AsyncDisposable(cancel)


class AsyncMulticastObservable(AsyncObservable):
    def __init__(self, subject, in_obs):
        super().__init__()
        
        self.in_obs = in_obs
        self.subject = subject

    def ref_count(self):
        return AsyncRefCountObservable(self)

    def connect(self):
        return subscribe(self.in_obs, self.subject)

    def __asubscribe__(self, observer):
        return subscribe(self.subject, observer)


class AsyncSubject(AsyncObservable, AsyncObserver):
    def __init__(self):
        super().__init__()

        self.observers = []

    async def __asubscribe__(self, observer):
        self.observers.append(observer)

        async def cancel():
            assert observer in self.observers, "Observer has already been unsubscribed"
            self.observers.remove(observer)

        return AsyncDisposable(cancel)

    async def asend(self, value) -> None:
        await parallel(observer.asend(value) for observer in self.observers)

    async def athrow(self, ex: Exception) -> None:
        await parallel(observer.athrow(ex) for observer in self.observers)

    async def aclose(self) -> None:
        await parallel(observer.aclose() for observer in self.observers)


def parallel(coros):
    return asyncio.gather(*coros)
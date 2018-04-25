import asyncio
from logzero import logger

from aioreactive.core import AsyncObservable, AsyncDisposable, AsyncObserver, subscribe


def multicast(subject):
    def inner(in_obs):
        return AsyncMulticastObservable(subject, in_obs)

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
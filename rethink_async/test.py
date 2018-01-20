import asyncio
from rethink_async import connection


async def do():
    conn = await connection("192.168.99.100", 28015)


def main():
    loop = asyncio.get_event_loop()

    loop.run_until_complete(do())
    loop.run_forever()
    loop.close()


if __name__ == "__main__":
    main()
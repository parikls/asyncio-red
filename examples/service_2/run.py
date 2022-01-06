import asyncio

from aioredis import Redis
from asyncio_red import RED, Via
from examples.service_2.red.registry.service_1 import EventViaList, EventViaChannel, EventViaStream


async def main():
    redis_cli = Redis()
    red = RED(app_name=str('service_2'), redis_cli=redis_cli)

    red.add_in(
        EventViaList,
        via=Via.LIST,
        list_name="list_events",
        handlers=[event_handler, ]
    )

    red.add_in(
        EventViaChannel,
        via=Via.CHANNELS,
        channel_name="channel_events",
        handlers=[event_handler, ]
    )

    red.add_in(
        EventViaStream,
        via=Via.STREAMS,
        stream_name="stream_events",
        group_name="group_events",
        consumer_name="consumer_name",
        handlers=[event_handler, event_handler]
    )

    asyncio.create_task(red.run())
    try:
        while True:
            await asyncio.sleep(1)
    finally:
        await red.close()


async def event_handler(event):
    print(event)


if __name__ == '__main__':
    asyncio.run(main())

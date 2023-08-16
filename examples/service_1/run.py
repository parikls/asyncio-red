import asyncio

from redis.asyncio import Redis

from asyncio_red import RED, Via
from examples.service_1.red.registry.service_1 import EventViaList, EventViaChannel, EventViaStream

redis_cli = Redis()
red = RED(app_name=str('service_1'), redis_cli=redis_cli)

# define how this particular event will be dispatched, e.g. using the redis list or
# via the redis channels or streams
red.add_out(
    EventViaList,
    via=Via.LIST,
    target_name="list_events"
)


red.add_out(
    EventViaChannel,
    via=Via.CHANNELS,
    target_name="channel_events"
)

red.add_out(
    EventViaStream,
    via=Via.STREAMS,
    target_name="stream_events"
)


async def your_awesome_function():
    ...  # do work

    # dispatch event in the code according to a router setup
    await EventViaList(value="list").dispatch()  # this one will be put to a list
    await EventViaChannel(value="channel").dispatch()  # this one will be pushed to a channel
    await EventViaStream(value="stream").dispatch()  # this one will be pushed to a stream


if __name__ == '__main__':
    asyncio.run(your_awesome_function())

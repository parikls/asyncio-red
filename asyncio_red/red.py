import asyncio
import json
import logging
import sys
from collections import defaultdict
from enum import IntEnum
from typing import Type, Generator, Optional, Dict, Callable, Union, Collection

import backoff
from aioredis import Redis, RedisError
from typing_extensions import Awaitable

from asyncio_red.consumer.base import BaseConsumer
from asyncio_red.consumer.channels import RedisChannelConsumer
from asyncio_red.consumer.lists import RedisListConsumer
from asyncio_red.consumer.streams import RedisStreamsConsumer
from asyncio_red.errors import RedError
from asyncio_red.events import Header, BaseEvent

logger = logging.getLogger('asyncio-red')


class Via(IntEnum):
    LIST = 1
    CHANNELS = 2
    STREAMS = 3


class RED:

    def __init__(self,
                 *,
                 app_name: str,
                 redis_cli: Optional[Redis] = None,
                 global_error_handler: Optional[Awaitable] = None,
                 **redis_kwargs) -> None:
        """
        :param app_name: name of your service/application/whatever. Will be used to identify same-service events
        :param redis_cli: prepared redis client. if not provided - we'll create a default one
        :param global_error_handler: awaitable which will be called on every error
        :param redis_kwargs: redis kwargs which will be used for creating the redis client (if one was not provided)
        """
        self.app_name = app_name
        self.redis_cli = redis_cli or Redis(**redis_kwargs)

        self.event_details = defaultdict(dict)

        # mapping of "via" type to actual consumers
        self.in_registry = {
            Via.LIST: {},
            Via.CHANNELS: {},
            Via.STREAMS: {},
        }
        self.global_error_handler = global_error_handler or self.default_error_handler
        self._task_by_consumer = {}

    async def run(self):
        """ Start events consuming """

        # start consumers
        for consumer in self._distinct_consumers():
            self._task_by_consumer[consumer] = asyncio.shield(consumer.start())

        # sleep forever
        delay = 1 if sys.platform == "win32" and sys.version_info < (3, 8) else 3600
        while True:
            await asyncio.sleep(delay)

    def add_out(self,
                event: Type[BaseEvent],
                via: Via,
                target_name: str,
                max_tries: int = 5):
        """
        Register outgoing event. Basically this method just assign appropriate dispatch method
        to an event and create a shortcut so event can be dispatched using next flow, e.g.:

        event = MyEvent(...)
        await event.dispatch()

        """
        retry_wrapper = backoff.on_exception(
            wait_gen=backoff.expo,
            exception=RedisError,
            max_tries=max_tries
        )

        def prepare_payload(event: BaseEvent) -> Dict:
            """ Prepares both header and body """
            header = Header(sender=self.app_name, name=event.__name__)
            return {"header": header.dict(), "body": event.dict()}

        def list_dispatch(event: BaseEvent):
            """ Dispatch to list """
            payload = prepare_payload(event)
            return self.redis_cli.lpush(target_name, json.dumps({'event': payload}))

        def channels_dispatch(event):
            """ Dispatch to channel """
            payload = prepare_payload(event)
            return self.redis_cli.publish(target_name, json.dumps({'event': payload}))

        def streams_dispatch(event):
            """ Dispatch to stream """
            payload = prepare_payload(event)
            return self.redis_cli.xadd(target_name, {'event': json.dumps(payload)})

        if via == Via.LIST:
            func = list_dispatch

        elif via == Via.CHANNELS:
            func = channels_dispatch

        elif via == Via.STREAMS:
            func = streams_dispatch

        else:
            raise RedError(f"Can't register outgoing event. Unknown `Via`: {via}")

        event.dispatch = retry_wrapper(func)

    def add_in(self,
               event: Type[BaseEvent],
               via: Via,
               handlers: Collection[Union[Awaitable, Callable]],
               error_handler: Optional[Union[Awaitable, Callable]] = None,
               **kwargs):
        """
        Register incoming event.

        :param event: pydantic event schema
        :param via: incoming channel, e.g. list, channel or stream
        :param handlers: list of awaitable which will be called when event will be received
        :param error_handler: awaitable which will handle the errors during handling
        :param kwargs: consumer kwargs. see particular consumer docs for more info
        """
        # persist event to a registry
        event_name = event.__name__

        if via == Via.LIST:
            consumer_key = kwargs['list_name']
            consumer_class = RedisListConsumer
        elif via == Via.CHANNELS:
            consumer_key = kwargs['channel_name']
            consumer_class = RedisChannelConsumer
        elif via == Via.STREAMS:
            stream_name = kwargs['stream_name']
            group_name = kwargs['group_name']
            consumer_name = kwargs['consumer_name']
            consumer_key = (stream_name, group_name, consumer_name)
            consumer_class = RedisStreamsConsumer
        else:
            raise RedError(f"Can't register incoming event. Unknown `Via`: {via}")

        consumer = self.in_registry[via].get(consumer_key)
        if not consumer:
            consumer = consumer_class(red=self, **kwargs)
            self.in_registry[via][consumer_key] = consumer

        consumer.register_event_schema(
            event_name=event_name,
            schema=event,
            handlers=handlers,
            error_handler=error_handler or self.default_event_error_handler
        )

    async def close(self):
        """ Closes processing and stops all consumers """
        for consumer, running_task in self._task_by_consumer.items():
            consumer.stop()
            await running_task

    def _distinct_consumers(self) -> Generator[BaseConsumer, None, None]:
        """ Yields all distinct consumers """
        for consumers in self.in_registry.values():
            for consumer in consumers.values():
                yield consumer

    @staticmethod
    async def default_error_handler(exc):
        logger.debug(f"Can't process event. Error: {exc}")
        logger.exception(f"Can't process event")

    @staticmethod
    async def default_event_error_handler(event, exc):
        logger.debug(f"Can't process event: {event}. Error: {exc}")
        logger.exception(f"Can't process event")

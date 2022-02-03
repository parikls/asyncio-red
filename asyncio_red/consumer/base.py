import logging
from asyncio import Event
from typing import Dict, Tuple, Any, Callable, Type, Collection

from aioredis import Redis
from pydantic import ValidationError

from asyncio_red.errors import InvalidEventError, EventHandlingError
from asyncio_red.events import Header, BaseEvent

logger = logging.getLogger('asyncio-red')


class BaseConsumer:

    async def start(self):
        """ This method must start the polling process """
        raise NotImplementedError()

    def stop(self, *args, **kwargs):
        """ This method must stop the polling process """
        self._is_stopping.set()

    def __init__(self,
                 *,
                 red: 'RED',
                 handle_same_origin: bool):

        self._red = red
        self._redis_cli: Redis = red.redis_cli
        self._app_name: str = red.app_name
        self._is_stopping = Event()
        self._handle_same_origin = handle_same_origin
        self._registry = {}

    def __str__(self):
        return f'{self.__class__.__name__}'

    __repr__ = __str__

    def _get_raw_event(self, redis_message) -> Dict:
        """ """
        raise NotImplementedError()

    def register_event_schema(self,
                              event_name: str,
                              schema: Type[BaseEvent],
                              handlers: Collection[Callable],
                              error_handler: Callable):
        self._registry[event_name] = {
            "schema": schema,
            "handlers": handlers,
            "error_handler": error_handler
        }

    def _parse_event(self, raw_event) -> Tuple[Header, BaseEvent]:
        """ Parses and validates raw event. Returns parsed header and event body """
        logger.debug(f'{self}: new raw event received: {raw_event}')
        try:
            header = Header(**raw_event['header'])
        except ValidationError as e:
            raise InvalidEventError(orig=e)

        # get event schema
        event_name = header.name
        event_schema = self._registry.get(event_name, {}).get("schema")
        if not event_schema:
            raise InvalidEventError(message=f"Can't find registered schema for event {event_name}")

        # parse event and return header and body
        try:
            body = event_schema(**raw_event['body'])
        except ValidationError as e:
            raise InvalidEventError(orig=e)

        return header, body

    async def _handle_event(self, header: Header, event: BaseEvent) -> Any:
        """ Calls handler for provided event """
        if header.sender == self._app_name and self._handle_same_origin is False:
            logger.debug(f'{self}: skipping same-origin event')
            return

        event_name = header.name
        handlers = self._registry[event_name]["handlers"]
        if not handlers:
            logger.debug(f'{self}: No handler configured for {event_name}')
            return

        for handler in handlers:
            try:
                await handler(event)
            except Exception as e:  # noqa
                # handle error
                red_error = EventHandlingError(
                    message=f"{self}: can't handle message", orig=e, header=header, event=event
                )
                error_handler = self._registry[event_name]["error_handler"]
                try:
                    await error_handler(event, red_error)
                except Exception:  # noqa
                    logger.exception(f"{self}: Unhandled error occurred during error-handling")

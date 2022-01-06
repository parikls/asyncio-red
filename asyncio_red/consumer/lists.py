import asyncio
import json
import logging
from asyncio import CancelledError
from datetime import timedelta
from typing import Dict, Optional

from aioredis import RedisError

from asyncio_red.consumer.base import BaseConsumer
from asyncio_red.errors import RedError, MalformedEventError, InvalidEventError

logger = logging.getLogger('asyncio-red')


class RedisListConsumer(BaseConsumer):
    """
    Implementation for consuming from redis lists
    """
    def __init__(self,
                 *,
                 red,
                 list_name: str,
                 handle_same_origin: Optional[bool] = False,
                 batch_interval: timedelta = timedelta(milliseconds=250)):
        """
        :param list_name: name of redis list
        :param: batch_interval: amount of time which consumer will sleep before next redis call
        """

        self._list_name = list_name
        self._batch_interval = batch_interval.total_seconds()
        super().__init__(red=red, handle_same_origin=handle_same_origin)

    async def start(self):
        """
        Polls event channel and process events with registered event handlers
        """
        while True:
            try:
                # sleep between batches
                await asyncio.sleep(self._batch_interval)

                # check closing state
                if self._is_stopping.is_set():
                    logger.info(f'{self}: cancelled redis polling')
                    return

                # get all pending items and execute handlers
                while True:
                    payload = await self._redis_cli.rpop(self._list_name)
                    if not payload:
                        break

                    # get raw event
                    try:
                        raw_event = self._get_raw_event(payload)
                    except Exception as e:
                        red_error = MalformedEventError(message=f"{self}: can't parse raw event", orig=e)
                        await self._red.global_error_handler(red_error)
                        continue

                    # parse and validate raw event
                    try:
                        header, event = self._parse_event(raw_event=raw_event)
                    except Exception as e:
                        red_error = InvalidEventError(message=f"{self}: can't parse header and body", orig=e)
                        await self._red.global_error_handler(red_error)
                        continue

                    # handle parsed event
                    await self._handle_event(header=header, event=event)

            except CancelledError:
                logger.info(f'{self}: cancelled redis polling')
                return

            except RedisError:
                logger.debug(f'{self}: redis error. reconnecting')
                continue

            except Exception:  # noqa
                logger.exception(f"{self}: Unhandled error occurred during redis polling")
                return

    def _get_raw_event(self, redis_message: str) -> Dict:
        """ Parses redis list message """
        try:
            decoded = json.loads(redis_message)
        except (TypeError, ValueError) as e:
            raise RedError(message="Can't parse event", orig=e)

        raw_event = decoded.get('event')
        if not raw_event:
            raise RedError("Can't get `event` property from in_registry event")

        return raw_event

import asyncio
import json
import logging
from asyncio import CancelledError
from typing import Dict, Optional

import backoff
from aioredis import RedisError

from asyncio_red.consumer.base import BaseConsumer
from asyncio_red.errors import InvalidEventError, MalformedEventError

logger = logging.getLogger('asyncio-red')


DEFAULT_RECONNECT_INTERVAL = 1.0
GET_MESSAGE_TIMEOUT = 5.0


class RedisChannelConsumer(BaseConsumer):
    """
    Implementation for consuming from redis channels
    """
    def __init__(self,
                 *,
                 red,
                 channel_name: str,
                 handle_same_origin: Optional[bool] = False):
        """
        :param channel_name: name of redis channel
        :param handle_same_origin: whether to handle the event sent by same app/service (app_name)
        """
        self._channel_name = channel_name
        super().__init__(red=red, handle_same_origin=handle_same_origin)

    async def start(self):
        """
        Polls event channel and process events with registered event handlers
        """
        reconnect_interval = backoff.expo(base=1.01, factor=1.01)
        while True:
            try:
                # subscribe to a channel
                pub_sub = self._redis_cli.pubsub()
                async with pub_sub as channel:
                    await channel.subscribe(self._channel_name)

                    # on successful connect - reset the reconnect interval
                    reconnect_interval = backoff.expo(base=1.01, factor=1.01)

                    while channel.subscribed:

                        # check closing state
                        if self._is_stopping.is_set():
                            logger.info(f'{self}: cancelled redis polling')
                            return

                        # get next message
                        message = await channel.get_message(
                            ignore_subscribe_messages=True, timeout=GET_MESSAGE_TIMEOUT
                        )
                        if not message:
                            continue

                        # get raw event
                        try:
                            raw_event = self._get_raw_event(message)
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
                # sleep before reconnect
                await asyncio.sleep(next(reconnect_interval))
                continue

            except Exception:  # noqa
                logger.exception(f"{self}: Unhandled error occurred during redis polling")
                return

    def _get_raw_event(self, redis_message: Dict) -> Dict:
        """ Parses redis channels message """
        decoded = json.loads(redis_message['data'])
        return decoded['event']

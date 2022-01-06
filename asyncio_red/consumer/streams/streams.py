import json
import logging
import time
from asyncio import sleep, Event, CancelledError
from collections import OrderedDict
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import List, Tuple, Dict, Optional
from uuid import uuid4

from aioredis import ResponseError, RedisError

from asyncio_red.consumer.base import BaseConsumer
from asyncio_red.errors import RedError, MalformedEventError, InvalidEventError

logger = logging.getLogger('asyncio-red')
here = Path(__file__).parent


class AckAfter(Enum):
    RECEIVE = "RECEIVE"
    HANDLE = "HANDLE"


STREAMS_FIRST_ID = '0-1'
STREAMS_ID_NEXT = '>'
STREAMS_MIN_ID = '-'
STREAMS_MAX_ID = '+'
BUSYGROUP_ERROR = 'BUSYGROUP'

MS = 1000


class RedisStreamsConsumer(BaseConsumer):

    def __init__(self,
                 *,
                 red,
                 stream_name: str,
                 group_name: str,
                 consumer_name: str = None,
                 handle_same_origin: Optional[bool] = False,
                 batch_interval: timedelta = timedelta(milliseconds=250),
                 x_claim_interval: timedelta = timedelta(minutes=30),
                 delete_processed_interval: Optional[timedelta] = timedelta(seconds=60),
                 delete_ack_interval: Optional[timedelta] = timedelta(seconds=30),
                 ack_after: AckAfter = AckAfter.RECEIVE,
                 x_read_block_delta: timedelta = timedelta(seconds=10),
                 x_read_count: int = 500):
        """
        :param stream_name: name of the redis stream
        :param group_name: group consumers name
        :param consumer_name: consumer name. by default this will be auto-generated using the uuid
        :param handle_same_origin: whether to process events sent from same origin (compare app_name to header.sender)
        :param batch_interval: interval between batch processing of messages
        :param x_claim_interval: interval between running the recovery task. by default it's 30 seconds and probably
                                 we shouldn't increase this value. If service was restarted but in the meantime there
                                 was lots of events - it is possible to die with OOM
        :param x_claim_delta: interval which is used to identify messages that was picked by other workers but were
                              not yet processed. by default we'll claim messages which were not processed for 30 minutes
        :param to_delete_processed: should we delete our processed messages? by default answer is yes
        :param to_delete_acknowledged: should we also delete ALL acked messages from this stream? Default - yes
        :param ack_after: we can either ACK messages immediately after receive or send ACK after handling. By default
                          we ACK after receiving.
        """

        self._stream_name = stream_name
        self._group_name = group_name
        self._consumer_name = consumer_name or uuid4().hex
        self._recovery_scheduled_at = datetime.now()
        self._x_del_candidates = []

        self._x_claim_delta = x_claim_interval.seconds * MS  # convert to ms
        self._x_read_block_delta = x_read_block_delta.seconds * MS  # convert to ms
        self._x_read_count = x_read_count

        if delete_ack_interval:
            self._delete_ack_interval = delete_ack_interval.total_seconds()
            self._delete_ack_at = time.time() + self._delete_ack_interval
        else:
            self._delete_ack_interval = self._delete_ack_at = None

        if delete_processed_interval:
            self._delete_processed_interval = delete_processed_interval.total_seconds()
            self._delete_processed_at = time.time() + self._delete_processed_interval
        else:
            self._delete_processed_interval = self._delete_processed_at = None

        if x_claim_interval:
            self._x_claim_interval = x_claim_interval.total_seconds()
            self._x_claim_at = time.time() + self._x_claim_interval
        else:
            self._x_claim_interval = self._x_claim_at = None

        self._batch_interval = batch_interval.total_seconds()
        self._ack_after = ack_after

        with (here / 'xtrim.lua').open('r') as f:
            self._x_trim_lua = f.read()

        self._closing = Event()
        self._closed = Event()

        super().__init__(red=red, handle_same_origin=handle_same_origin)

    async def start(self):
        """ Single tick of processing """

        logger.info(f'{self}: start listening to {self._stream_name}')
        await self._create_group()

        while True:

            try:
                await self._delete_all_acknowledged()
                await self._delete_processed()
                await self._claim_pending()
                await self._process_messages(start_id=STREAMS_ID_NEXT)

            except CancelledError:
                logger.info(f'{self}: cancelled redis polling')
                return

            except RedisError:
                logger.debug(f'{self}: redis error. reconnecting')
                continue

            except Exception:  # noqa
                logger.exception(f'{self}: Unhandled error occurred')
                continue

            finally:
                # check if we are not closing the processing so far
                if self._is_stopping.is_set():
                    return

                # small sleep to avoid 100% CPU load
                await sleep(self._batch_interval)

    async def _process_messages(self, start_id):
        """
        Entry-point for message processing. Fetching all messages
        from a stream starting from `start_id`, dispatches them
        to an output queue and persist their ids in a collection
        for future deletion of acked ones
        """

        try:
            messages = await self._redis_cli.xreadgroup(
                groupname=self._group_name,
                consumername=self._consumer_name,
                streams={self._stream_name: start_id},
                block=self._x_read_block_delta,
                count=self._x_read_count
            )

            if not messages:
                return

            logger.info(f'Processing next {len(messages)} message(s)')
            await self._process_batch(messages=messages)

        # todo: look into this
        # except ReplyError:
        except Exception as e:
            logger.error('Re-creating consumer group')
            await self._create_group()
            return

        # except Exception:
        #     logger.exception(f'{self}: message processing failed')

    def _get_raw_event(self, redis_message) -> Dict:
        decoded = {k.decode('utf-8'): v.decode('utf-8') for k, v in redis_message.items()}
        try:
            raw_event = json.loads(decoded['event'])
        except (TypeError, ValueError) as e:
            raise RedError(message="Can't parse event", orig=e)

        return raw_event

    async def _process_batch(self, messages: List[Tuple[bytes, bytes, OrderedDict]]):
        """ Process single batch of messages """
        for _, message in messages:
            for message_id, payload in message:
                message_id = message_id.decode('utf-8')

                # get raw event
                try:
                    raw_event = self._get_raw_event(payload)
                except Exception as e:
                    red_error = MalformedEventError(message=f"{self}: can't parse raw event", orig=e)
                    await self._red.global_error_handler(red_error)
                    return

                # parse and validate raw event
                try:
                    header, event = self._parse_event(raw_event=raw_event)
                except Exception as e:
                    red_error = InvalidEventError(message=f"{self}: can't parse header and body", orig=e)
                    await self._red.global_error_handler(red_error)
                    return

                await self._ack(current_phase=AckAfter.RECEIVE, message_id=message_id)
                await self._handle_event(header=header, event=event)
                await self._ack(current_phase=AckAfter.HANDLE, message_id=message_id)

    async def _ack(self, current_phase, message_id):
        if self._ack_after != current_phase:
            return
        await self._redis_cli.xack(self._stream_name, self._group_name, message_id)
        if self._delete_processed_interval:
            self._x_del_candidates.append(message_id)

    async def _claim_pending(self):
        """
        Recovery task. Acquire messages which were picked by other workers, but were not ACKed.
        Sometimes this can happen when worker dies after picking the messages from the stream.
        """
        if not self._x_claim_interval:
            return

        now = time.time()
        if now <= self._x_claim_at:
            return

        logger.info(f'{self}: running claim process')
        try:
            # todo: probably we should fetch pending messages
            # todo: till we have them, not just a batch of 500

            # fetch pending messages which were not yet ACKed
            pending_messages = await self._redis_cli.xpending_range(
                name=self._stream_name,
                groupname=self._group_name,
                min=STREAMS_MIN_ID,
                max=STREAMS_MAX_ID,
                count=self._x_read_count
            )

            if pending_messages:
                # pending messages exist - if they are too old - we collect them
                to_be_claimed = []
                for message in pending_messages:
                    consumer_name = message['consumer'].decode('utf-8')
                    if consumer_name == self._consumer_name:
                        # this message is still processing by this consumer. no need to claim
                        continue
                    if message['time_since_delivered'] > self._x_claim_delta:
                        to_be_claimed.append(message['message_id'].decode('utf-8'))

                if to_be_claimed:
                    # use an X-CLAIM operation to assign pending messages to this worker
                    await self._redis_cli.xclaim(
                        name=self._stream_name,
                        groupname=self._group_name,
                        consumername=self._consumer_name,
                        min_idle_time=self._x_claim_delta,
                        message_ids=to_be_claimed
                    )

                    # process messages from the very beginning to be able to get
                    # all previously claimed messages
                    await self._process_messages(start_id=STREAMS_FIRST_ID)

        except Exception:  # noqa
            logger.exception(f'{self}: claim failed')

        finally:
            self._x_claim_at = time.time() + self._x_claim_interval

    async def _delete_all_acknowledged(self):
        """
        Cleanup of all (!) ACKed messages which we have in the stream.
        """
        if not self._delete_ack_interval:
            return

        now = time.time()
        if now <= self._delete_ack_at:
            return

        try:
            await self._redis_cli.eval(self._x_trim_lua, 1, *(self._stream_name,))
            logger.info(f'{self}: {self._stream_name}. ACK cleanup succeed')
        except Exception:  # noqa
            logger.exception(f'{self}: ACK cleanup failed')
        finally:
            self._delete_ack_at = time.time() + self._delete_ack_interval

    async def _delete_processed(self):
        """
        Cleanup of already processed ACKed messages by this particular worker.
        """
        if not self._delete_processed_interval:
            return

        now = time.time()
        if now <= self._delete_processed_at:
            return

        try:
            if not self._x_del_candidates:
                return

            logger.info(f'{self}: deleting {len(self._x_del_candidates)} messages')
            try:
                await self._redis_cli.xdel(self._stream_name, *self._x_del_candidates)
            except Exception:  # noqa
                logger.exception('Unhandled error occurred during deletion of processed messages')
            else:
                self._x_del_candidates = []

        finally:
            # set next run timestamp
            self._delete_processed_at = time.time() + self._delete_processed_interval

    async def _create_group(self):
        """ Helper method for creating stream group """
        try:
            await self._redis_cli.xgroup_create(
                name=self._stream_name,
                groupname=self._group_name,
                mkstream=True
            )
        except ResponseError as e:
            if BUSYGROUP_ERROR in str(e):
                logger.info(f'Stream group {self._group_name} already exists')
                pass  # stream group already exist
            else:
                raise

        except Exception:  # noqa
            logger.exception("Can't create stream consumer group")
            raise

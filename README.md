asyncio-RED (Redis Event Driven)
================================

Powers your microservices with event driven approach using redis as a backend.

Support both publishing and subscribing using lists, channels and streams.

`pydantic` is being used for events validation.

`s3` can be used for sharing event schemas between services.

Installation
------------

- `pip install asyncio-red`


Simple producer
---------------

```python
from redis.asyncio import Redis
from asyncio_red import RED, Via, BaseEvent
from pydantic import Field

class EventV1List(BaseEvent):
    key: str = Field(..., title='Key description')
    

class EventV1Channel(BaseEvent):
    key: str = Field(..., title='Key description')
    
    
class EventV1Stream(BaseEvent):
    key: str = Field(..., title='Key description')

    
redis_client = Redis()
red = RED(app_name=str('service_1'), redis_client=redis_client)

red.add_out(
    event=EventV1List,
    via=Via.LIST,
    target_name='events_list'
)

red.add_out(
    event=EventV1Channel,
    via=Via.CHANNELS,
    target_name='events_channel'
)

red.add_out(
    event=EventV1Stream,
    via=Via.STREAMS,
    target_name='events_stream'
)


async def your_awesome_function():
    # dispatch events
    await EventV1List(key='value').dispatch()  # this one will be put to a list
    await EventV1Channel(key='value').dispatch()  # this one will be pushed to a channel
    await EventV1Stream(key='value').dispatch()  # this one will be pushed to a stream
```


Simple consumer
--------------

```python
from redis.asyncio import Redis
from asyncio_red import RED, Via, BaseEvent
from pydantic import Field

class EventV1List(BaseEvent):
    key: str = Field(..., title='Key description')
    

class EventV1Channel(BaseEvent):
    key: str = Field(..., title='Key description')
    
    
class EventV1Stream(BaseEvent):
    key: str = Field(..., title='Key description')
    

redis_client = Redis()
red = RED(app_name=str('service_2'), redis_client=redis_client)


async def event_handler(event):
    print(event)


red.add_in(
    event=EventV1List,
    via=Via.LIST,
    handlers=(event_handler, ),
    list_name="events_list",
)

red.add_in(
    event=EventV1Channel,
    via=Via.CHANNELS,
    handlers=(event_handler, ),
    error_handler=event_handler,
    channel_name="events_channel"
)

red.add_in(
    event=EventV1Stream,
    via=Via.STREAMS,
    handlers=(event_handler, event_handler),
    stream_name="events_stream",
    group_name="events_group",
    consumer_name="consumer_name"
)

await red.run()
```


Shared events registry
----------------------

There is a possibility to keep event schemas registry on the S3 and share the schema across 
different services. You'll need an AWS account and keys with access to S3.

- Go to app root dir and initialize asyncio-red:

```shell
asyncio_red init --app-name=<app name> --s3-bucket=<bucket name>
```

This will create an initial structure.
Define your events at `red/registry/<app name>.py`:

```python
from pydantic import Field
from asyncio_red.events import BaseEvent


class EventV1List(BaseEvent):
    key: str = Field(..., title='Key description')
    

class EventV1Channel(BaseEvent):
    key: str = Field(..., title='Key description')
    
    
class EventV1Stream(BaseEvent):
    key: str = Field(..., title='Key description')

```

- push application events schemas to a registry: `asyncio-red push`
- on a different service you can pull shared schemas - do the same steps, e.g. init structure and run `asyncio-red pull`



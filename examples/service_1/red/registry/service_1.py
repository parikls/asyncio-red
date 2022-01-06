from asyncio_red import BaseEvent


# write your schemas extending the `BaseEvent`
class EventViaList(BaseEvent):
    value: str


class EventViaChannel(BaseEvent):
    value: str


class EventViaStream(BaseEvent):
    value: str

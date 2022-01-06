from typing import Optional

from asyncio_red.events import Header, BaseEvent


class RedError(Exception):
    """ Base RED exception """

    def __init__(self, message: Optional[str] = None, orig: Optional[Exception] = None):
        self.message = message
        self.orig = orig


class MalformedEventError(RedError):
    """ Malformed event error """


class InvalidEventError(RedError):
    """ Invalid event received """


class EventHandlingError(RedError):
    """ Can't handle event """
    def __init__(self,
                 message: str,
                 orig: Exception,
                 header: Header,
                 event: BaseEvent):
        super().__init__(message=message, orig=orig)
        self.header = header
        self.event = event

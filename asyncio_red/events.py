import time
from uuid import uuid4

from pydantic import BaseModel, Field


class Header(BaseModel):
    """ Event header with all the required fields """
    id: str = Field(default_factory=lambda: uuid4().hex)
    time: float = Field(default_factory=time.time)
    sender: str = Field(...)
    name: str = Field(...)


class BaseEvent(BaseModel):
    """ Base Event """

    async def dispatch(self):
        """ This method is being set dynamically in the router """
        pass

from pydantic import BaseModel
from enum import Enum

class EventType(str, Enum):
    EMAIL = "user.email"
    SMS = "user.sms"
    PUSH = "user.push"
    ANALYTICS = "system.analytics"

class NotificationEvent(BaseModel):
    event_type: EventType
    user_id: int
    email: str | None = None
    phone: str | None = None
    device_id: str | None = None
    message: str

from enum import Enum
from pydantic import BaseModel, EmailStr, Field


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


class EmailEvent(BaseModel):
    event_type: str = Field(pattern=r"user\.email")
    user_id: int
    email: EmailStr
    message: str


class SmsEvent(BaseModel):
    event_type: str = Field(pattern=r"user\.sms")
    user_id: int
    phone: str
    message: str
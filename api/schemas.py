from enum import Enum
from pydantic import BaseModel, EmailStr, Field, model_validator, field_validator
import re

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
    message: str = Field(
        ...,
        min_length=1,
        description="Message must not be empty"
    )

    @model_validator(mode="after")
    def validate_by_event_type(self):
        match self.event_type:
            case EventType.EMAIL:
                if not self.email:
                    raise ValueError("email is required for user.email event")

            case EventType.SMS:
                if not self.phone:
                    raise ValueError("phone is required for user.sms event")

            case EventType.PUSH:
                if not self.device_id:
                    raise ValueError("device_id is required for user.push event")

            case EventType.ANALYTICS:
                pass  # только message обязателен

        return self


class EmailEvent(BaseModel):
    event_type: str = Field(pattern=r"user\.email")
    user_id: int
    email: EmailStr
    message: str


class SmsEvent(BaseModel):
    event_type: str = Field(pattern=r"user\.sms")
    user_id: int
    phone: str = Field(..., min_length=10, max_length=16)  # базовая валидация длины
    message: str = Field(..., min_length=1)

    @field_validator("phone")
    def check_phone(cls, v):

        if not re.fullmatch(r"^\+\d{10,15}$", v):
            raise ValueError("Phone must be in international format, e.g., +71234567890")
        return v
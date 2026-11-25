from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class IngestEventRequest(BaseModel):
    delivery_id: Optional[str] = Field(
        default=None,
        description="Unique event/delivery ID. If omitted, a UUID will be generated."
    )
    source: str = Field(default="fake-generator", description="Source of the event")
    event_type: str = Field(..., description="Type of event, e.g., order.created, user.signup")
    data: Dict[str, Any] = Field(default_factory=dict, description="Arbitrary event payload")


class IngestResponse(BaseModel):
    status: str = Field(..., description="'accepted' or 'duplicate'")
    delivery_id: str
    message: str


class RawEventResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    delivery_id: str
    source: str
    event_type: str
    payload: Dict[str, Any]
    status: str
    received_at: datetime
    outbox_published: bool = True
    normalized_payload: Optional[Dict[str, Any]] = None


class ProcessedEventResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    raw_event_id: int
    delivery_id: str
    event_type: str
    normalized_data: Dict[str, Any]
    processed_at: datetime


class EventDetailResponse(BaseModel):
    raw_event: RawEventResponse
    processed_record: Optional[ProcessedEventResponse] = None
    failure_history: List[Dict[str, Any]] = []


class PaginatedEventsResponse(BaseModel):
    items: List[RawEventResponse]
    total: int
    limit: int
    offset: Optional[int] = 0
    next_cursor: Optional[str] = None
    has_more: bool = False


class ReplayResponse(BaseModel):
    status: str
    event_id: int
    delivery_id: str
    message: str


class RetentionCleanupResponse(BaseModel):
    outbox_deleted: int
    raw_events_deleted: int
    message: str


class ResetIngestlyRequest(BaseModel):
    confirm: str = Field(..., description="Must be 'clear-ingestly' to confirm full reset")


class ResetIngestlyResponse(BaseModel):
    failed_deleted: int
    processed_deleted: int
    outbox_deleted: int
    raw_events_deleted: int
    message: str

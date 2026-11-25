from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional, Set

from sqlalchemy import JSON, Boolean, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class EventStatus(str, Enum):
    RECEIVED = "received"
    QUEUED = "queued"
    PROCESSING = "processing"
    PROCESSED = "processed"
    FAILED = "failed"
    REPLAYED = "replayed"


VALID_STATUS_TRANSITIONS: Dict[str, Set[str]] = {
    EventStatus.RECEIVED: {EventStatus.QUEUED, EventStatus.PROCESSING, EventStatus.FAILED},
    EventStatus.QUEUED: {EventStatus.PROCESSING, EventStatus.FAILED},
    EventStatus.PROCESSING: {EventStatus.PROCESSED, EventStatus.FAILED},
    EventStatus.PROCESSED: set(),
    EventStatus.FAILED: {EventStatus.RECEIVED, EventStatus.REPLAYED},
    EventStatus.REPLAYED: {EventStatus.QUEUED, EventStatus.PROCESSING, EventStatus.FAILED},
}


def can_transition(current: str, target: str) -> bool:
    return target in VALID_STATUS_TRANSITIONS.get(current, set())


class Base(DeclarativeBase):
    pass


class RawEvent(Base):
    __tablename__ = "raw_events"
    __table_args__ = (
        UniqueConstraint("delivery_id", "source", name="uq_raw_events_delivery_source"),
        Index("ix_raw_events_received_at_id", "received_at", "id"),
        Index("ix_raw_events_status_received_id", "status", "received_at", "id"),
        Index("ix_raw_events_received_at", "received_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    delivery_id: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    source: Mapped[str] = mapped_column(String(64), index=True, default="fake-generator")
    event_type: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    payload: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String(32), index=True, default="received")  # received, queued, processing, processed, failed
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    processed_record: Mapped[Optional["ProcessedEvent"]] = relationship(
        "ProcessedEvent", back_populates="raw_event", uselist=False, cascade="all, delete-orphan"
    )

    @property
    def normalized_payload(self) -> Optional[Dict[str, Any]]:
        return self.processed_record.normalized_data if self.processed_record else None


class OutboxEvent(Base):
    __tablename__ = "outbox_events"
    __table_args__ = (
        Index("ix_outbox_events_published_id", "published", "id"),
        Index("ix_outbox_events_published_created_at", "published", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    raw_event_id: Mapped[int] = mapped_column(Integer, ForeignKey("raw_events.id", ondelete="CASCADE"), nullable=False, index=True)
    payload: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    published: Mapped[bool] = mapped_column(Boolean, default=False, index=True)


class ProcessedEvent(Base):
    __tablename__ = "processed_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    raw_event_id: Mapped[int] = mapped_column(Integer, ForeignKey("raw_events.id"), nullable=False, unique=True)
    delivery_id: Mapped[str] = mapped_column(String(128), unique=True, index=True, nullable=False)
    event_type: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    normalized_data: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    raw_event: Mapped["RawEvent"] = relationship("RawEvent", back_populates="processed_record")


class FailedEvent(Base):
    __tablename__ = "failed_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    raw_event_id: Mapped[int] = mapped_column(Integer, ForeignKey("raw_events.id"), nullable=False)
    delivery_id: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    error_message: Mapped[str] = mapped_column(Text, nullable=False)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    failed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


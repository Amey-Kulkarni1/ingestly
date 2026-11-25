"""Initial schema with query and outbox indexes

Revision ID: 0001_initial_schema
Revises:
Create Date: 2026-11-25 00:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0001_initial_schema"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "raw_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("delivery_id", sa.String(length=128), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("event_type", sa.String(length=128), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("delivery_id", "source", name="uq_raw_events_delivery_source"),
    )
    op.create_index("ix_raw_events_delivery_id", "raw_events", ["delivery_id"])
    op.create_index("ix_raw_events_source", "raw_events", ["source"])
    op.create_index("ix_raw_events_event_type", "raw_events", ["event_type"])
    op.create_index("ix_raw_events_status", "raw_events", ["status"])
    op.create_index("ix_raw_events_received_at", "raw_events", ["received_at"])
    op.create_index("ix_raw_events_received_at_id", "raw_events", ["received_at", "id"])
    op.create_index("ix_raw_events_status_received_id", "raw_events", ["status", "received_at", "id"])

    op.create_table(
        "outbox_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("raw_event_id", sa.Integer(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published", sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(["raw_event_id"], ["raw_events.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_outbox_events_raw_event_id", "outbox_events", ["raw_event_id"])
    op.create_index("ix_outbox_events_published", "outbox_events", ["published"])
    op.create_index("ix_outbox_events_published_id", "outbox_events", ["published", "id"])
    op.create_index("ix_outbox_events_published_created_at", "outbox_events", ["published", "created_at"])

    op.create_table(
        "processed_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("raw_event_id", sa.Integer(), nullable=False),
        sa.Column("delivery_id", sa.String(length=128), nullable=False),
        sa.Column("event_type", sa.String(length=128), nullable=False),
        sa.Column("normalized_data", sa.JSON(), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["raw_event_id"], ["raw_events.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("delivery_id"),
        sa.UniqueConstraint("raw_event_id"),
    )
    op.create_index("ix_processed_events_delivery_id", "processed_events", ["delivery_id"])
    op.create_index("ix_processed_events_event_type", "processed_events", ["event_type"])

    op.create_table(
        "failed_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("raw_event_id", sa.Integer(), nullable=False),
        sa.Column("delivery_id", sa.String(length=128), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=False),
        sa.Column("retry_count", sa.Integer(), nullable=False),
        sa.Column("failed_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["raw_event_id"], ["raw_events.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_failed_events_delivery_id", "failed_events", ["delivery_id"])


def downgrade() -> None:
    op.drop_table("failed_events")
    op.drop_table("processed_events")
    op.drop_table("outbox_events")
    op.drop_table("raw_events")


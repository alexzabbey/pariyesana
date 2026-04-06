from datetime import datetime, timezone

from sqlalchemy import DateTime, Index, Integer, String, Text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Talk(Base):
    __tablename__ = "talks"

    talk_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    date: Mapped[str] = mapped_column(String(10), default="")
    title: Mapped[str] = mapped_column(Text, default="")
    teacher: Mapped[str] = mapped_column(String(200), default="", index=True)
    teacher_id: Mapped[str] = mapped_column(String(20), default="")
    center: Mapped[str] = mapped_column(String(200), default="", index=True)
    duration: Mapped[str] = mapped_column(String(20), default="")
    description: Mapped[str] = mapped_column(Text, default="")
    mp3_url: Mapped[str] = mapped_column(Text, default="")
    language: Mapped[str] = mapped_column(String(50), default="English", index=True)
    status: Mapped[str] = mapped_column(String(20), default="pending", index=True)
    claimed_by: Mapped[str | None] = mapped_column(String(100), nullable=True)
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        Index("ix_talks_status_claimed_at", "status", "claimed_at"),
    )

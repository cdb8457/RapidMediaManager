import datetime
import uuid
from sqlalchemy import String, DateTime
from sqlalchemy.orm import Mapped, mapped_column
from media_manager.database import Base

class WebhookLog(Base):
    __tablename__ = "webhook_logs"
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    status: Mapped[str] = mapped_column(String, default="received")
    event: Mapped[str] = mapped_column(String, nullable=True)
    media_title: Mapped[str] = mapped_column(String, nullable=True)
    timestamp: Mapped[datetime.datetime] = mapped_column(DateTime, default=datetime.datetime.utcnow)

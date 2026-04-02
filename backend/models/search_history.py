"""
SQLAlchemy ORM model for search_history table.
"""
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.sql import func
from database import Base


class SearchHistory(Base):
    __tablename__ = "search_history"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    country = Column(String, nullable=False)
    states = Column(String, nullable=True)  # JSON string of state codes
    result_count = Column(Integer, nullable=True, default=0)
    searched_at = Column(DateTime(timezone=True), server_default=func.now())

    def to_dict(self):
        import json
        return {
            "id": self.id,
            "user_id": self.user_id,
            "country": self.country,
            "states": json.loads(self.states) if self.states else [],
            "result_count": self.result_count,
            "searched_at": self.searched_at.isoformat() if self.searched_at else None,
        }

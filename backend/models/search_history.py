"""
SQLAlchemy ORM model for search_history table.

Stores complete scraped result data (as a JSON blob) alongside
search metadata, so history items can be re-displayed instantly
without triggering a new scrape.
"""
import json
from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey
from sqlalchemy.sql import func
from database import Base


class SearchHistory(Base):
    __tablename__ = "search_history"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    country = Column(String, nullable=False)
    states = Column(String, nullable=True)          # JSON string of state codes
    result_count = Column(Integer, nullable=True, default=0)
    # Full cached dataset — JSON array of business dicts captured at scrape time
    result_data = Column(Text, nullable=True)
    # Pagination metadata (total_pages, limit, total_records)
    pagination_meta = Column(String, nullable=True)
    # SHA1 of country + sorted-states used for duplicate detection
    search_key = Column(String(40), nullable=True, index=True)
    searched_at = Column(DateTime(timezone=True), server_default=func.now())

    def to_dict(self, include_data: bool = False):
        state_list = []
        if self.states:
            try:
                state_list = json.loads(self.states)
            except (json.JSONDecodeError, TypeError):
                state_list = []
        
        pagination = {}
        if self.pagination_meta:
            try:
                pagination = json.loads(self.pagination_meta)
            except (json.JSONDecodeError, TypeError):
                pagination = {}

        result = {
            "id": self.id,
            "user_id": self.user_id,
            "country": self.country,
            "states": state_list,
            "result_count": self.result_count,
            "pagination_meta": pagination,
            "has_data": bool(self.result_data),
            "searched_at": self.searched_at.isoformat() if self.searched_at else None,
        }

        if include_data:
            if self.result_data:
                try:
                    result["result_data"] = json.loads(self.result_data)
                except (json.JSONDecodeError, TypeError):
                    result["result_data"] = None
                    result["data_error"] = "Data corrupted"
            else:
                result["result_data"] = None

        return result

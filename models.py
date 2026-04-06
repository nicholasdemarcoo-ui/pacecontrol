from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship

from db import Base


class TeeSheet(Base):
    __tablename__ = "tee_sheets"

    id = Column(Integer, primary_key=True)
    sheet_date = Column(String, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    rows = relationship(
        "TeeSheetRow",
        back_populates="tee_sheet",
        cascade="all, delete-orphan"
    )


class TeeSheetRow(Base):
    __tablename__ = "tee_sheet_rows"

    id = Column(Integer, primary_key=True)
    tee_sheet_id = Column(Integer, ForeignKey("tee_sheets.id"), nullable=False, index=True)

    reservation_time = Column(String, default="")
    group_name = Column(String, default="")
    players = Column(String, default="")
    num_players = Column(Integer, default=0)
    walkers = Column(String, default="")
    riders = Column(String, default="")
    front = Column(String, default="")
    back = Column(String, default="")
    rotation = Column(String, default="")
    total_time = Column(String, default="")
    average_hole = Column(String, default="")

    tee_sheet = relationship("TeeSheet", back_populates="rows")
#DATABASE

from typing import Any
from sqlalchemy import Column, ForeignKey, Integer, String, Float, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.types import Boolean
Base = declarative_base()


def to_dict(obj: Base) -> dict[str, Any]:
    return {c.name: getattr(obj, c.name) for c in obj.__table__.columns}

class Fen(Base):
    __tablename__ = "fen"
    fen = Column(String, primary_key=True)
    depth = Column(Integer, nullable = False)
    seldepth = Column(Integer, nullable = False)
    time = Column(Float, nullable = False)
    nodes = Column(Integer, nullable = False)
    score = Column(Float, nullable = False)
    tbhits = Column(String, nullable = False)
    node_per_second = Column(Integer, nullable = False)
    

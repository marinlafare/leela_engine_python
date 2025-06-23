#DATABASE

from typing import Any
from sqlalchemy import Column, ForeignKey, Integer, String, Float, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.types import Boolean
Base = declarative_base()


def to_dict(obj: Base) -> dict[str, Any]:
    return {c.name: getattr(obj, c.name) for c in obj.__table__.columns}
    
class Rawfen(Base):
    __tablename__ = "rawfen"
    fen = Column(String, primary_key = True)
class Fen(Base):
    __tablename__ = "fen"
    fen = Column(String, primary_key=True)
    depth = Column(Integer, nullable = False)
    seldepth = Column(Integer, nullable = False)
    time = Column(Float, nullable = False)
    nodes = Column(Integer, nullable = False)
    score = Column(Float, nullable = False)
    tbhits = Column(Integer, nullable = False)
    nps = Column(Integer, nullable = False)
    
class Knownfens(Base):
    __tablename__ = "knownfens"
    link = Column(BigInteger,primary_key = True, unique = True)
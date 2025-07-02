
#DATABASE_MODELS

from typing import Any
from sqlalchemy import Column, ForeignKey, Integer, String, Float, BigInteger, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.types import Boolean
Base = declarative_base()


def to_dict(obj: Base) -> dict[str, Any]:
    return {c.name: getattr(obj, c.name) for c in obj.__table__.columns}

class Fen(Base):
    __tablename__ = "fen"
    fen = Column(String, primary_key = True)
    n_games = Column(BigInteger, nullable = False)
    moves_counter = Column(String, nullable = False)
    score = Column('score', Float, nullable = True)
    
class FromGame(Base):
    __tablename__ = "from_game"
    link = Column('link',BigInteger, primary_key=True, index=True)
    
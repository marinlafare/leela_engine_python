
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
    fen = Column('fen',String, primary_key = True, index = True, unique = True)
    n_games = Column('n_games',BigInteger, nullable = False)
    moves_counter = Column('moves_counter',String, nullable = False)
    next_moves = Column('next_moves',String, nullable = True)
    score = Column('score', Float, nullable = True)
    
class FromGame(Base):
    __tablename__ = "from_game"
    link = Column('link',BigInteger, primary_key=True, index=True)
    
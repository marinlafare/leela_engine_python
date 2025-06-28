#DATABASE_MODELS

from typing import Any
from sqlalchemy import Column, ForeignKey, Integer, String, Float, BigInteger, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.types import Boolean
Base = declarative_base()


def to_dict(obj: Base) -> dict[str, Any]:
    return {c.name: getattr(obj, c.name) for c in obj.__table__.columns}

class MainFen(Base):
    __tablename__ = "main_fen"
    fen = Column(String, primary_key = True)
    n_games = Column(BigInteger, nullable = False)
    moves_counter = Column(String, nullable = False)

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
class ProcessedGame(Base):
    __tablename__ = "processed_game"
    link = Column('link',BigInteger, primary_key=True, index=True)
    analyzed = Column('analyzed',Boolean,nullable=False,unique=False)

class GameFen(Base):
    __tablename__ = "game_fen"
    id = Column(Integer, primary_key=True, autoincrement=True)
    link = Column("link",BigInteger,ForeignKey("processed_game.link"),
                  nullable=False,unique=False)
    n_move = Column('n_move',Float, nullable = False)
    fen = Column("fen", String, nullable=False)
    game = relationship(ProcessedGame, foreign_keys=[link])
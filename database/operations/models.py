# OPERATIONS_MODELS
from typing import Optional
from pydantic import BaseModel

class FenCreateData(BaseModel):
    fen: str
    n_games: int
    moves_counter: str
    next_moves: Optional[str] = None
    score: Optional[float] = None
class FromGameCreateData(BaseModel):
    link: int
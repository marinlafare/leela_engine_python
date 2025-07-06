# OPERATIONS_MODELS
from typing import Optional, List
from pydantic import BaseModel

class FenCreateData(BaseModel):
    fen: str
    n_games: int
    moves_counter: str
    next_moves: Optional[str] = None
    score: Optional[float] = None

class FenGameAssociateData(BaseModel):
    fen: str  # The FEN string to associate
    links: List[int]
    
# database.operations.models.py

from typing import Optional, List
from pydantic import BaseModel

class FenCreateData(BaseModel):
    fen: str
    n_games: int
    moves_counter: str
    next_moves: Optional[str] = None
    score: Optional[float] = None

class FenGameAssociateData(BaseModel):
    fen_fen: str  # The FEN string to associate
    game_link: int
class AnalysisTimesCreateData(BaseModel):
    batch_index: int
    n_batches:int
    card: int
    model: str
    n_fens: int
    time_elapsed: float
    fens_per_second:float
    analyse_time_limit:float
    nodes_limit:int
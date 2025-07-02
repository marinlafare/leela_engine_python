# OPERATIONS_MODELS

from pydantic import BaseModel

class FenCreateData(BaseModel):
    fen: str
    n_games: int
    moves_counter: str
    score: float = None
class ProcessedGameCreateData(BaseModel):
    link: int
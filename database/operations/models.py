# OPERATIONS_MODELS

from pydantic import BaseModel
class FenCreateData(BaseModel):
    fen: str
    depth: int
    seldepth: int
    time: float
    nodes: int
    score: float
    tbhits: int
    nps: int
class MainFenCreateData(BaseModel):
    fen: str
    n_games: int
    moves_counter: str
class ProcessedGameCreateData(BaseModel):
    link: int
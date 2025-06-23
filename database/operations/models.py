# OPERATIONS

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

class KnownfensCreateData(BaseModel):
    link: int
class RawfenCreateData(BaseModel):
    fen:str
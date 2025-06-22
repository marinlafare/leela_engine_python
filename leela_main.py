import os
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket
from contextlib import asynccontextmanager
from constants import CONN_STRING
from database.database.engine import init_db
from database.routers import fen
from database.operations.collect_fens import collect_fens
# lifespan event handler for new implementation
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db(CONN_STRING)
    print('...LEELA Server ON...')
    yield
    print('...LEELA Server DOWN YO!...')

app = FastAPI(lifespan=lifespan)

@app.get("/")
def read_root():
    return "LEELA server running."

app.include_router(fen.router)
#app.include_router(collect_fens.router)


@app.websocket("/ws/collect_fens/{n_games}")
def api_collect_fens(websocket: WebSocket,n_games:str):
    result = collect_fens(websocket,n_games)
    return result
    
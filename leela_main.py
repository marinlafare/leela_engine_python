import os
from dotenv import load_dotenv
from fastapi import FastAPI
from contextlib import asynccontextmanager
from constants import CONN_STRING
from database.database.engine import init_db
from database.routers import fen

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
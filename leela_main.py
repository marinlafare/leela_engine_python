# leela_main.py

import sys 
import os
from constants import *
import os
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket
from contextlib import asynccontextmanager
from database.database.engine import init_db, AsyncDBSession

from database.routers import fen, collect_fens

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Initializes the asynchronous database connection.
    """
    print('Initializing LEELA Server...')
    try:
        # Call the asynchronous init_db
        await init_db(CONN_STRING)
        print('...LEELA chessism Server ON...')
        yield
        # Add cleanup logic here if needed (e.g., closing engine)
        # For now, FastAPI handles graceful shutdown of async connections by default
    except Exception as e:
        print(f"Failed to start LEELA Server due to database initialization error: {e}")
        # Optionally re-raise or handle more gracefully based on desired app behavior
        raise # Re-raise to prevent server from starting if DB init fails

    print('...LEELA Chessism Server DOWN YO!...')

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def read_root(): # Changed to async def as good practice for FastAPI endpoints
    return "LEELA server running."

# Include your existing routers
app.include_router(fen.router)
app.include_router(collect_fens.router)


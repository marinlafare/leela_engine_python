# LEELA MAIN (main.py)
import os
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket
from contextlib import asynccontextmanager
from constants import CONN_STRING

# Import the new asynchronous init_db and AsyncDBSession
from database.database.engine import init_db, AsyncDBSession

# Import your routers
from database.routers import fen, collect_fens

# Load environment variables (important for CONN_STRING)
load_dotenv('this_is_not_an_env.env')

# Lifespan event handler for new asynchronous implementation
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Handles startup and shutdown events for the FastAPI application.
    Initializes the asynchronous database connection.
    """
    print('Initializing LEELA Server...')
    try:
        # Call the asynchronous init_db
        await init_db(CONN_STRING)
        print('...LEELA Server ON...')
        yield
        # Add cleanup logic here if needed (e.g., closing engine)
        # For now, FastAPI handles graceful shutdown of async connections by default
    except Exception as e:
        print(f"Failed to start LEELA Server due to database initialization error: {e}")
        # Optionally re-raise or handle more gracefully based on desired app behavior
        raise # Re-raise to prevent server from starting if DB init fails

    print('...LEELA Server DOWN YO!...')

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def read_root(): # Changed to async def as good practice for FastAPI endpoints
    return "LEELA server running."

# Include your existing routers
app.include_router(fen.router)
app.include_router(collect_fens.router)

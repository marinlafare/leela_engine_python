# DATABASE_ASK_DB
import os
import requests
import pandas as pd
import tempfile
from itertools import chain
from typing import List, Dict, Any
from constants import CONN_STRING # CONN_STRING should now be 'postgresql+asyncpg://'

# Import necessary SQLAlchemy async components
from sqlalchemy import text, select # For executing raw SQL or ORM selects asynchronously
from database.database.engine import AsyncDBSession # For getting async sessions
from database.database.models import MainFen, Fen, ProcessedGame # Import models if you plan to use ORM for these


async def open_async_request(sql_question: str, params: dict = None, fetch_as_dict: bool = False):
    """
    Executes an asynchronous SQL query, optionally with parameters, and fetches results.
    Uses AsyncDBSession for connection management.
    """
    async with AsyncDBSession() as session:
        try:
            # Use text() for raw SQL queries and bindparams for parameters
            if params:
                result = await session.execute(text(sql_question), params)
            else:
                result = await session.execute(text(sql_question))
            
            if fetch_as_dict:
                # Fetch all rows as SQLAlchemy Row objects, then convert to dicts
                rows = result.fetchall()
                return [row._mapping for row in rows]
            else:
                # Return list of tuples by default
                return result.fetchall()
        except Exception as e:
            await session.rollback() # Ensure rollback on error
            print(f"Error in open_async_request: {e}")
            raise # Re-raise to propagate the error


async def get_one_game(link: int) -> List[Dict[str, Any]]: # Updated type hints
    """
    Retrieves move data for a single game link asynchronously.
    """
    sql_query = "SELECT n_move, white_move, black_move FROM moves WHERE link = :link"
    # Parameters for text() must be dict-like
    return await open_async_request(sql_query, params={"link": link}, fetch_as_dict=True)

async def get_all_tables() -> List[tuple]: # Updated type hints
    """
    Retrieves a list of all base table names in the 'public' schema asynchronously.
    """
    sql_query = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    AND table_type = 'BASE TABLE';
    """
    return await open_async_request(sql_query) # No params, fetches as tuples by default

async def delete_all_leela_tables():
    """
    Deletes specified Leela-related tables asynchronously.
    Note: DDL operations like DROP TABLE automatically commit.
    """
    async with AsyncDBSession() as session:
        for table_name_to_delete in ['fen','main_fen','processed_game']:
            print(f"Deleting table: {table_name_to_delete}...")
            try:
                # Use text() for DDL commands
                await session.execute(text(f"DROP TABLE IF EXISTS \"{table_name_to_delete}\" CASCADE;"))
                # No explicit commit needed for DDL in async context as it's auto-committed by the DB
                print(f"Successfully deleted table: {table_name_to_delete}")
            except Exception as e:
                # Rollback is not strictly necessary for DDL that fails, but good general practice
                await session.rollback()
                print(f"An unexpected error occurred during deletion of {table_name_to_delete}: {e}")
                # Don't re-raise immediately if you want to try deleting other tables
                # But for a script, re-raising might be desired for immediate feedback
        # A single commit for the session at the end, though individual DDLs are often auto-committed.
        await session.commit()
    print("All specified Leela tables deletion attempt complete.")

async def get_new_games_links(n_games: int) -> List[tuple]: # Updated type hints
    """
    Retrieves new game links from the 'game' table that have not yet been
    processed (i.e., not present in 'processed_game').
    Uses a LEFT JOIN ... WHERE IS NULL for efficient querying with large datasets asynchronously.
    """
    sql_query = f"""
    SELECT g.link
    FROM game AS g
    LEFT JOIN processed_game AS pgl ON g.link = pgl.link
    WHERE pgl.link IS NULL
    LIMIT :n_games;
    """
    # Use params for LIMIT to ensure type safety even with f-string for query structure
    return await open_async_request(sql_query, params={"n_games": n_games})


# --- FUNCTIONS TO FETCH COUNTS ---

async def get_total_game_count() -> str: # Changed return type to str
    """
    Fetches the total number of game links in the 'game' table asynchronously,
    formatted with commas.
    """
    sql_query = "SELECT COUNT(link) FROM game;"
    result = await open_async_request(sql_query)
    count = result[0][0] if result else 0
    return f"{count:,}" # Format with commas

async def get_processed_game_count() -> str: # Changed return type to str
    """
    Fetches the total number of processed game links in the 'processed_game' table asynchronously,
    formatted with commas.
    """
    sql_query = "SELECT COUNT(link) FROM processed_game;"
    result = await open_async_request(sql_query)
    count = result[0][0] if result else 0
    return f"{count:,}" # Format with commas

async def get_main_fen_count() -> str: # Changed return type to str
    """
    Fetches the total number of unique FENs in the 'main_fen' table asynchronously,
    formatted with commas.
    """
    sql_query = "SELECT COUNT(fen) FROM main_fen;"
    result = await open_async_request(sql_query)
    count = result[0][0] if result else 0
    return f"{count:,}" # Format with commas

async def get_unanalyzed_fens(limit: int) -> List[str]:
    """
    Fetches a list of FENs from 'main_fen' that do not exist in the 'fen' table (unanalyzed).
    
    Args:
        limit (int): The maximum number of unanalyzed FENs to retrieve.

    Returns:
        List[str]: A list of FEN strings that need analysis.
    """
    sql_query = """
    SELECT mf.fen
    FROM main_fen AS mf
    LEFT JOIN fen AS f ON mf.fen = f.fen
    WHERE f.fen IS NULL
    LIMIT :limit;
    """
    result_tuples = await open_async_request(sql_query, params={"limit": limit})
    # The result will be a list of tuples, e.g., [('fen1',), ('fen2',)]
    return [t[0] for t in result_tuples]

# DATABASE_ASK_DB
import os
import requests
import pandas as pd
import tempfile
from itertools import chain
from typing import List, Dict, Any
from constants import CONN_STRING



from sqlalchemy import text, select, update
from database.database.engine import AsyncDBSession
from database.database.models import Fen, Game

async def open_async_request(sql_question: str,
                             params: dict = None,
                             fetch_as_dict: bool = False):
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
        for table_name_to_delete in ['fen','game_fen_association']:
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
async def get_game_links_by_username(username: str, limit: int = 100) -> List[int]: # Added limit parameter
    """
    Fetches game links from the 'game' table for a given username,
    excluding those already marked as processed in 'processed_game_links',
    up to a specified limit.

    Args:
        username (str): The username to search for.
        limit (int): The maximum number of game links to fetch.

    Returns:
        List[int]: A list of new, unprocessed game links (integers) for the user.
    """
    sql_query = """
    SELECT g.link
    FROM game AS g
    LEFT JOIN processed_game AS pgl ON g.link = pgl.link
    WHERE (g.white = :username OR g.black = :username) AND pgl.link IS NULL
    LIMIT :limit; -- Added LIMIT clause
    """
    # Use open_async_request and await it. Fetch values directly as links.
    result_tuples = await open_async_request(sql_query, params={"username": username, "limit": limit})
    # Extract just the link (which is the first and only element in each tuple)
    return [link[0] for link in result_tuples]
async def get_fens_by_game_link(game_link: int) -> List[Dict[str, Any]]:
    """
    Retrieves all FENs associated with a specific game link.

    Args:
        game_link (int): The link (primary key) of the game to retrieve FENs for.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, where each dictionary
                              contains the 'fen' string associated with the game.
    """
    sql_query = """
        SELECT
            f.fen
        FROM
            game AS g
        INNER JOIN
            game_fen_association AS gfa ON g.link = gfa.game_link
        INNER JOIN
            fen AS f ON gfa.fen_fen = f.fen
        WHERE
            g.link = :game_link_param;
    """
    result = await open_async_request(
        sql_query,
        params={"game_link_param": game_link},
        fetch_as_dict=True
    )
    return result
async def get_games_by_fen(fen_string: str) -> List[Dict[str, Any]]:
    """
    Retrieves all games associated with a specific FEN string.

    Args:
        fen_string (str): The FEN string to retrieve associated games for.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, where each dictionary
                              represents a game associated with the provided FEN,
                              containing only the 'link'.
    """
    sql_query = """
        SELECT
            g.link
        FROM
            fen AS f
        INNER JOIN
            game_fen_association AS gfa ON f.fen = gfa.fen_fen
        INNER JOIN
            game AS g ON gfa.game_link = g.link
        WHERE
            f.fen = :fen_param;
    """
    result = await open_async_request(
        sql_query,
        params={"fen_param": fen_string},
        fetch_as_dict=True
    )
    return result

async def get_players_with_names() -> List[Dict[str, Any]]:
    """
    Retrieves all player records where the 'name' column is not NULL,
    returning only their player_name.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, where each dictionary
                              represents a player with a non-NULL name,
                              containing only the 'player_name'.
    """
    sql_query = """
        SELECT
            player_name
        FROM
            player
        WHERE
            name IS NOT NULL;
    """
    result = await open_async_request(
        sql_query,
        fetch_as_dict=True
    )
    return result
async def reset_player_game_fens_done_to_false(player_name: str) -> int:
    """
    Resets the 'fens_done' column to False for all Game records associated with a specific player.
    This applies to games where the player is either white or black.
    Intended for development and testing phases to easily reset game processing status for a player.

    Args:
        player_name (str): The player_name (username) for whom to reset 'fens_done' status.

    Returns:
        int: The number of rows that were updated.
    """
    if not player_name:
        print("Player name cannot be empty. No games will be reset.")
        return 0

    async with AsyncDBSession() as session:
        try:
            # Construct the UPDATE statement to set fens_done to False
            # for games where the given player is either white or black.
            stmt = (
                update(Game)
                .where(
                    (Game.white == player_name) | (Game.black == player_name)
                )
                .values(fens_done=False)
            )

            # Execute the update statement
            result = await session.execute(stmt)

            # Commit the transaction
            await session.commit()

            # Return the number of rows affected by the update
            print(f"Successfully reset 'fens_done' to False for {result.rowcount} game(s) involving player '{player_name}'.")
            return result.rowcount

        except Exception as e:
            await session.rollback()
            print(f"An error occurred while resetting 'fens_done' status for player '{player_name}': {e}")
            raise # Re-raise the exception after rollback to propagate the error
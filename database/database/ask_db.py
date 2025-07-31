# DATABASE_ASK_DB
import os
import requests
import tempfile
from itertools import chain
from typing import List, Dict, Any
from constants import CONN_STRING

from sqlalchemy import text, select, update
from database.database.engine import async_engine, AsyncDBSession
from database.database.models import Fen, Game, AnalysisTimes
from database.database.db_interface import DBInterface
from database.database.engine import init_db

async def get_all_database_names():
    """
    Fetches the names of all databases accessible by the current connection.
    This function needs to be adapted based on your specific database type.
    """
    # NO: await init_db(CONN_STRING) here. It must be called once at application startup.
    # We now assume async_engine has been initialized by init_db before this function is called.
    
    # Check if async_engine is None, which indicates init_db was not called yet.
    await init_db(CONN_STRING)
    if async_engine is None:
        print("Error: Database engine not initialized. Call init_db() first.")
        # Optionally raise an error or return an empty list
        raise RuntimeError("Database engine not initialized. Call init_db() first.")

    dialect_name = async_engine.dialect.name

    query = ""
    if "postgresql" in dialect_name:
        query = "SELECT datname FROM pg_database WHERE datistemplate = false;"
    elif "mysql" in dialect_name:
        query = "SHOW DATABASES;"
    elif "sqlite" in dialect_name:
        query = "PRAGMA database_list;"
    elif "mssql" in dialect_name: # SQL Server
        query = "SELECT name FROM sys.databases;"
    else:
        print(f"Warning: Database dialect '{dialect_name}' not explicitly handled for listing databases.")
        return []

    print(f"Querying databases for {dialect_name} using: {query}")
    try:
        results = await open_async_request(query, fetch_as_dict=True)
        if results:
            db_names = []
            for row in results:
                if "sqlite" in dialect_name:
                    db_names.append(row['name'])
                elif 'datname' in row:
                    db_names.append(row['datname'])
                elif 'name' in row:
                    db_names.append(row['name'])
                elif 'Database' in row:
                    db_names.append(row['Database'])
            return db_names
        return []
    except Exception as e:
        print(f"Error fetching database names: {e}")
        return []

async def open_async_request(sql_question: str,
                             params: dict = None,
                             fetch_as_dict: bool = False):
    """
    Executes an asynchronous SQL query, optionally with parameters, and fetches results.
    Uses AsyncDBSession for connection management.
    """
    # Ensure your AsyncDBSession is using a SQLAlchemy AsyncSession
    async with AsyncDBSession() as session: # session is now an AsyncSession instance
        try:
            # Use text() for raw SQL queries and bindparams for parameters
            if params:
                result = await session.execute(text(sql_question), params)
            else:
                result = await session.execute(text(sql_question))

            # Check if the statement is expected to return rows.
            sql_upper = sql_question.strip().upper()
            if sql_upper.startswith("DROP TABLE") or \
               sql_upper.startswith("CREATE TABLE") or \
               sql_upper.startswith("ALTER TABLE") or \
               sql_upper.startswith("TRUNCATE TABLE"):
                # DDL operations do not return rows.
                print(f"DDL operation '{sql_question}' executed successfully (no rows returned).")
                # Explicitly commit the transaction for DDL operations
                # The AsyncDBSession context manager also handles this on exit,
                # but explicit is often clearer for DDL.
                # await session.commit() # This might be redundant if __aexit__ commits
                return None

            if fetch_as_dict:
                rows = result.fetchall()
                return [row._mapping for row in rows]
            else:
                return result.fetchall()
        except ResourceClosedError as e:
            # This specific error indicates a non-row-returning statement was executed.
            print(f"Warning: Attempted to fetch rows from a non-row-returning statement: {sql_question}. Error: {e}")
            return None
        except Exception as e:
            await session.rollback() # Ensure rollback on error
            print(f"Error in open_async_request: {e}")
            raise # Re-raise to propagate the error
# async def open_async_request(sql_question: str,
#                              params: dict = None,
#                              fetch_as_dict: bool = False):
#     """
#     Executes an asynchronous SQL query, optionally with parameters, and fetches results.
#     Uses AsyncDBSession for connection management.
#     """
#     async with AsyncDBSession() as session:
#         try:
#             # Use text() for raw SQL queries and bindparams for parameters
#             if params:
#                 result = await session.execute(text(sql_question), params)
#             else:
#                 result = await session.execute(text(sql_question))
            
#             if fetch_as_dict:
#                 # Fetch all rows as SQLAlchemy Row objects, then convert to dicts
#                 rows = result.fetchall()
#                 return [row._mapping for row in rows]
#             else:
#                 # Return list of tuples by default
#                 return result.fetchall()
#         except Exception as e:
#             await session.rollback() # Ensure rollback on error
#             print(f"Error in open_async_request: {e}")
#             raise # Re-raise to propagate the error

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


async def delete_analysis_times():
    """
    Deletes specified Leela-related tables asynchronously.
    Note: DDL operations like DROP TABLE automatically commit.
    """
    async with AsyncDBSession() as session:
        print(f"Deleting table: analysis_times ...")
        try:
            # Use text() for DDL commands
            await session.execute(text(f"DROP TABLE IF EXISTS analysis_times CASCADE;"))
            # No explicit commit needed for DDL in async context as it's auto-committed by the DB
            print(f"Successfully deleted table: analysis_times")
        except Exception as e:
            # Rollback is not strictly necessary for DDL that fails, but good general practice
            await session.rollback()
            print(f"An unexpected error occurred during deletion of analysis_times: {e}")
            # Don't re-raise immediately if you want to try deleting other tables
            # But for a script, re-raising might be desired for immediate feedback
    # A single commit for the session at the end, though individual DDLs are often auto-committed.
        await session.commit()
    print("analysis_times table deletion attempt complete.")

async def save_analysis_times(batch_data):
    print('________')
    analysis_times_interface = DBInterface(AnalysisTimes)
    await analysis_times_interface.create(batch_data)
    print('_________')
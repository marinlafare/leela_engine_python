import os
import chess
import chess.engine
import asyncio
import time
import psutil
import json
import re
import logging
from sqlalchemy import text, select
import subprocess # Required for get_system_metrics if it's within this file or common utils
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Set
from database.database.engine import AsyncDBSession
# Configure logging for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) # Set to INFO for less verbose output. Change to logging.DEBUG for more detail.

# Ensure these imports are correct based on your project structure
from constants import LC0_PATH, lc0_directory, LC0_WEIGHTS_FILE
from database.database.db_interface import DBInterface
from database.database.models import Fen, MainFen, ProcessedGame # All relevant models
from database.database.ask_db import open_async_request
from database.operations.models import FenCreateData, MainFenCreateData, ProcessedGameCreateData

async def open_async_request(sql_question: str, params: dict = None, fetch_as_dict: bool = False):
    """
    Executes an asynchronous SQL query, optionally with parameters, and fetches results.
    Uses AsyncDBSession for connection management.
    """
    async with AsyncDBSession() as session:
        try:
            if params:
                result = await session.execute(text(sql_question), params)
            else:
                result = await session.execute(text(sql_question))
            
            if fetch_as_dict:
                rows = result.fetchall()
                return [row._mapping for row in rows] # _mapping is for SQLAlchemy Row objects
            else:
                return result.fetchall()
        except Exception as e:
            await session.rollback()
            logging.error(f"Error in open_async_request: {e}")
            raise # Re-raise to propagate the error

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) # Set to INFO for less verbose output. Change to logging.DEBUG for more detail.

async def get_game_links_by_username(username: str, limit: int = 10) -> list[str]:
    """
    Fetches new game links for a given user from the database's 'game' table,
    Filters games that hasn't been processed.
    """
    try:
        logger.info(f"Attempting to fetch {limit} new game links for user '{username}' from the database...")
        FETCH_MULTIPLIER = 2 # Fetch 5x the requested limit to ensure enough new links remain after filtering
        all_fetched_links_rows = await open_async_request(
            """
            SELECT g.link
            FROM game AS g
            WHERE g.white = :username OR g.black = :username
            LIMIT :query_limit;
            """,
            params={"username": username, "query_limit": limit * FETCH_MULTIPLIER}
        )
        all_fetched_links_list = [row[0] for row in all_fetched_links_rows]
        
        if not all_fetched_links_list:
            logger.info(f"No game links found in the database for user '{username}'.")
            return []

        # --- OPTIMIZED FILTERING FOR PROCESSED GAMES ---
        if all_fetched_links_list: # Only query if there are links to check
            existing_processed_links_rows = await open_async_request(
                """
                SELECT link FROM processed_game WHERE link = ANY(:links_to_check);
                """,
                params={"links_to_check": all_fetched_links_list}
            )
            existing_processed_links_set = {row[0] for row in existing_processed_links_rows}
        else:
            existing_processed_links_set = set()
        all_fetched_links_set = set(all_fetched_links_list)
        new_links = list(all_fetched_links_set - existing_processed_links_set)

        logger.info(f"Fetched {len(all_fetched_links_list)} candidate links from DB. {len(new_links)} are new (filtered {len(all_fetched_links_list) - len(new_links)} already processed).")

        return new_links[:limit]

    except Exception as e:
        logger.error(f"Error fetching game links for user {username} from DB: {e}", exc_info=True)
        return []


async def get_all_moves_for_links_batch(game_links: list[str]) -> dict[str, list[dict]]:
    """
    Fetches game moves for a batch of game links from the database.
    Returns a dictionary mapping game link to its list of moves.
    """
    moves_data_batch = {x:[] for x in game_links}
    if not game_links:
        return {}

    logger.info(f"Fetching moves for {len(game_links)} game links from the database...")
    
    # SQL query to select the link and the moves data for the given game links
    # Replace 'moves_json_or_text_field' with the actual column name in your 'game' table
    # where the game moves are stored (e.g., as a JSON string or JSONB).
    game_moves_rows = await open_async_request(
        """
        SELECT link, n_move, white_move, black_move
        FROM moves
        WHERE link = ANY(:game_links);
        """,
        params={"game_links": game_links}
    )

    for link, n_move, white_move, black_move in game_moves_rows:
        moves_data_batch[link].append({
            'n_move': n_move,
            'white_move': white_move,
            'black_move': black_move
        })

    logger.info(f"Finished fetching and structuring moves for {len(moves_data_batch)} games.")
    return moves_data_batch


def generate_fens_for_single_game_moves(moves: list[dict]) -> list[tuple[str, dict]]:
    """
    Generates a sequence of FENs and initial empty counters dictionary for a single chess game
    given its moves. Includes validation for the resulting FENs.
    Returns an empty list if an invalid move or FEN is encountered.

    Args:
        moves (list[dict]): A list of dictionaries, where each dictionary represents
                            a move with keys 'n_move', 'white_move', 'black_move'.

    Returns:
        list[tuple[str, dict]]: A list of (FEN string, empty dictionary) tuples
                                representing the board state after each half-move.
                                Returns an empty list if an invalid move or malformed FEN is found.
    """
    board = chess.Board()
    fens_sequence_with_counters = []

    for ind, key in enumerate(moves.keys()):
        move = moves[key]
        expected_move_num = ind + 1
        current_move_num = move.get('n_move')
        # Robustness checks for move data integrity
        if current_move_num is None:
            logger.debug(f"Skipping game (move missing 'n_move' key): {move}")
            return []
        if current_move_num != expected_move_num:
            logger.debug(f"Skipping game (inconsistent move order): Expected n_move {expected_move_num}, got {current_move_num} for move {move}")
            return []

        n_move = current_move_num
        white_move_san = move.get('white_move')
        black_move_san = move.get('black_move')

        # Process White's move
        if white_move_san:
            try:
                move_obj_white = board.parse_san(white_move_san)
                board.push(move_obj_white)
                current_fen = board.fen()
                try:
                    # Validate the generated FEN
                    _ = chess.Board(current_fen)
                    fens_sequence_with_counters.append((current_fen, {}))
                    logger.debug(f"Generated FEN (White, Move {n_move}): {current_fen}")
                except ValueError as e:
                    logger.debug(f"Invalid FEN generated (White, Move {n_move}): {current_fen} - Error: {e}")
                    return []
            except (ValueError, chess.InvalidMoveError) as e:
                logger.debug(f"Error applying White's move '{white_move_san}' at move number {n_move}: {e}")
                return []

        # Process Black's move (only if black_move exists and white's move was successful)
        if black_move_san:
            try:
                move_obj_black = board.parse_san(black_move_san)
                board.push(move_obj_black)
                current_fen = board.fen()
                try:
                    # Validate the generated FEN
                    _ = chess.Board(current_fen)
                    fens_sequence_with_counters.append((current_fen, {}))
                    logger.debug(f"Generated FEN (Black, Move {n_move}): {current_fen}")
                except ValueError as e:
                    logger.debug(f"Invalid FEN generated (Black, Move {n_move}): {current_fen} - Error: {e}")
                    return []
            except (ValueError, chess.InvalidMoveError) as e:
                logger.debug(f"Error applying Black's move '{black_move_san}' at move number {n_move}: {e}")
                return []

    return fens_sequence_with_counters


def simplify_fen_and_extract_counters_for_insert(raw_fen: str, initial_counters: dict) -> MainFenCreateData:
    """
    Simplifies a FEN string by removing move counters and fullmove number,
    and prepares data for MainFen insertion.
    The initial_counters dictionary is expected to be empty or contain default values.
    This function sets initial n_games and moves_counter to 1 for a new observation
    within a batch, which will later be aggregated by insert_fens.
    """
    parts = raw_fen.split(' ')
    # The first four parts are board, active color, castling availability, en passant target square
    simplified_fen = ' '.join(parts[:4])
    
    # These counters represent the observation from *this* game,
    # and will be aggregated with existing data in `insert_fens`.
    return MainFenCreateData(
        fen=simplified_fen,
        n_games=1,
        moves_counter=1
    )


async def insert_fens(all_raw_fens_with_counters: list[tuple[str, dict]]):
    """
    Inserts or updates FENs in the 'main_fen' table.
    It aggregates multiple occurrences of the same FEN from the current batch
    and then performs a bulk upsert (insert or update).
    
    Args:
        all_raw_fens_with_counters (list[tuple[str, dict]]): A list of (raw_fen, empty_dict) tuples
                                                    from `generate_fens_for_single_game_moves`.
    """
    main_fen_db_interface = DBInterface(MainFen)

    aggregated_fens_for_batch = {}
    for raw_fen, _ in all_raw_fens_with_counters: # Unpack and ignore the dummy dict
        # Simplify FEN for consistent aggregation key
        simplified_fen_data = simplify_fen_and_extract_counters_for_insert(raw_fen, {})
        simplified_fen = simplified_fen_data.fen

        if simplified_fen in aggregated_fens_for_batch:
            # If FEN already seen in this batch, increment its local counters
            aggregated_fens_for_batch[simplified_fen]['n_games'] += 1
            aggregated_fens_for_batch[simplified_fen]['moves_counter'] += 1
        else:
            # If new FEN in this batch, initialize counters
            aggregated_fens_for_batch[simplified_fen] = {
                'n_games': 1,
                'moves_counter': 1
            }

    logger.info(f"Aggregated {len(all_raw_fens_with_counters)} raw FENs down to {len(aggregated_fens_for_batch)} unique FENs for batch processing.")

    data_for_upsert = []
    for simplified_fen, counts in aggregated_fens_for_batch.items():
        data_for_upsert.append(MainFenCreateData(
            fen=simplified_fen,
            n_games=counts['n_games'],
            moves_counter=counts['moves_counter']
        ).model_dump()) # Convert Pydantic model to dict for bulk insert

    if data_for_upsert:
        try:
            # Use DBInterface's create_all which now handles MainFen upsert
            success = await main_fen_db_interface.create_all(data_for_upsert)
            if success:
                logger.info(f"Successfully processed (inserted/updated) {len(data_for_upsert)} unique FENs in 'main_fen'.")
            else:
                logger.error(f"Failed to process FENs in 'main_fen' for this batch.")
        except Exception as e:
            logger.error(f"Error during bulk upsert of FENs into 'main_fen': {e}", exc_info=True)
    else:
        logger.info("No FENs to insert/update in 'main_fen' for this batch.")


async def insert_processed_game_links(processed_game_links: list[str]):
    """
    Inserts processed game links into the 'processed_game' table.
    Expects a list of game link strings.
    """
    if not processed_game_links:
        logger.info("No processed game links to insert.")
        return

    processed_game_db_interface = DBInterface(ProcessedGame)
    # Convert list of strings to list of Pydantic models (then to dict for insertion)
    data_to_insert = [ProcessedGameCreateData(link=link).model_dump()
                      for link in processed_game_links]

    try:
        # DBInterface.create_all for ProcessedGame now handles ON CONFLICT DO NOTHING
        success = await processed_game_db_interface.create_all(data_to_insert)
        if success:
            logger.info(f"Successfully inserted {len(data_to_insert)} game links into processed_game.")
        else:
            logger.warning(f"Failed to insert processed game links for {len(data_to_insert)} games.")
    except Exception as e:
        logger.error(f"Error inserting processed game links: {e}", exc_info=True)


async def get_fens_from_games_optimized(game_links: list[str]) -> list[tuple[str, dict]]:
    """
    Processes a list of game links, fetches their moves, and generates a list of
    (FEN, counters_dict) tuples. This is a batch-oriented function.
    """
    all_fens_with_counters = []
    
    # 1. Get moves for the batch of game links
    moves_data_batch = await get_all_moves_for_links_batch(game_links)
    logger.info(f"Retrieved moves for {len(moves_data_batch)} games.")
    
    # 2. Generate FENs for each game in the batch
    for game_link, moves in moves_data_batch.items():
        if not moves:
            logger.warning(f"Skipping FEN generation for {game_link} as no moves were found.")
            continue
        
        game_fens_with_counters = generate_fens_for_single_game_moves(moves)
        if game_fens_with_counters:
            all_fens_with_counters.extend(game_fens_with_counters)
        else:
            logger.warning(f"No valid FENs generated for game {game_link} due to invalid moves or FENs (check debug logs).")
            
    return all_fens_with_counters


async def get_new_fens(all_generated_fens_with_counters: list[tuple[str, dict]]) -> list[tuple[str, dict]]:
    """
    Filters a list of generated FENs, returning only those that do not
    already exist in the 'main_fen' table.
    """
    if not all_generated_fens_with_counters:
        logger.info("No FENs generated to check for newness.")
        return []

    # Extract simplified FEN strings for database lookup (only need the FEN string for comparison)
    simplified_fens_generated = {
        simplify_fen_and_extract_counters_for_insert(fen_tuple[0], {}).fen
        for fen_tuple in all_generated_fens_with_counters
    }

    if not simplified_fens_generated:
        logger.info("No unique simplified FENs to check against the database.")
        return []

    # Query the database for existing FENs
    existing_fens_in_db_rows = await open_async_request(
        """
        SELECT fen FROM main_fen WHERE fen = ANY(:fens);
        """,
        params={"fens": list(simplified_fens_generated)}
    )
    existing_fens_set = {row[0] for row in existing_fens_in_db_rows}

    # Filter out FENs that already exist in the database, preserving the original tuple structure
    new_fens_filtered = []
    for fen_tuple in all_generated_fens_with_counters:
        fen_str = fen_tuple[0]
        simplified_fen = simplify_fen_and_extract_counters_for_insert(fen_str, {}).fen
        if simplified_fen not in existing_fens_set:
            new_fens_filtered.append(fen_tuple)
            
    logger.info(f"Found {len(new_fens_filtered)} truly new FENs out of {len(all_generated_fens_with_counters)} generated.")
    return new_fens_filtered


async def collect_fens_operations(n_games: int, username: str = 'lafareto') -> Dict[str, Any]:
    """
    Orchestrates the collection of FENs from new games for a specific user,
    and inserts them into the 'main_fen' table.
    Does NOT perform analysis with Lc0.
    
    Args:
        n_games (int): The number of new games to attempt to process.
        username (str): The username for whom to collect games.

    Returns:
        Dict[str, Any]: A dictionary containing status and summary information.
    """
    logger.info(f"\n--- Starting FEN Collection Operations for User: {username} ---")
    start_total_process = time.time()

    # 1. Get new game links
    game_links_start_time = time.time()
    new_game_links = await get_game_links_by_username(username, n_games)
    game_links_end_time = time.time()
    logger.info(f"Time to fetch {len(new_game_links)} NEW game links: {game_links_end_time - game_links_start_time:.4f} seconds")

    if not new_game_links:
        logger.info("No new game links found to collect FENs from. Exiting collection process.")
        return {"status": "no_new_games", "fens_collected": 0, "games_processed": 0, "duration_seconds": 0.0}

    # 2. Get FENs from games
    start_fens_gen = time.time()
    all_fens_with_counters_from_new_games = await get_fens_from_games_optimized(new_game_links)
    end_fens_gen = time.time()
    logger.info(f"Generated {len(all_fens_with_counters_from_new_games)} FENs from {len(new_game_links)} games in {end_fens_gen - start_fens_gen:.4f} seconds.")

    # 3. Filter for truly new FENs (not already in main_fen)
    start_new_fens_check = time.time()
    new_fens_to_insert = await get_new_fens(all_fens_with_counters_from_new_games)
    end_new_fens_check = time.time()
    logger.info(f"Identified {len(new_fens_to_insert)} truly new FENs to insert in {end_new_fens_check - start_new_fens_check:.4f} seconds.")

    # 4. Insert/Update new FENs into the 'main_fen' table
    logger.info(f"\n--- Inserting {len(new_fens_to_insert)} FENs into the database (main_fen) ---")
    start_insert_fens = time.time()
    await insert_fens(new_fens_to_insert) # This handles aggregation and upsert
    end_insert_fens = time.time()
    logger.info(f"FEN insertion/update to main_fen time elapsed: {end_insert_fens - start_insert_fens:.4f} seconds")

    # 5. Mark game links as processed
    logger.info(f"\n--- Marking {len(new_game_links)} game links as processed ---")
    start_insert_games = time.time()
    await insert_processed_game_links(new_game_links)
    end_insert_games = time.time()
    logger.info(f"Processed game links insertion time elapsed: {end_insert_games - start_insert_games:.4f} seconds")

    end_total_process = time.time()
    total_duration = end_total_process - start_total_process

    response_message = (
        f"{len(new_fens_to_insert)} NEW FENS from {len(new_game_links)} NEW GAMES "
        f"processed in {total_duration:.4f} seconds."
    )
    logger.info(f"--- FEN Collection Operations Complete for User: {username} ---")
    logger.info(response_message)
    
    return {
        "status": "completed",
        "fens_collected": len(new_fens_to_insert),
        "games_processed": len(new_game_links),
        "duration_seconds": total_duration,
        "message": response_message
    }

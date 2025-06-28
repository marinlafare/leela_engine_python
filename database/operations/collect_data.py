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
                return [row._mapping for row in rows]
            else:
                return result.fetchall()
        except Exception as e:
            await session.rollback()
            logging.error(f"Error in open_async_request: {e}")
            raise

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
        if all_fetched_links_list: 
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


async def generate_fens_for_single_game_moves(moves_data_batch: dict[int]) -> list[tuple[str, dict]]:
    simplify = simplify_fen_and_extract_counters_for_insert
    
    precessed_games_data = dict()
    fens_sequence_with_counters = []
    
    
    for link in moves_data_batch.keys():
        one_game_moves = moves_data_batch[link]
        precessed_games_data[link] = []
        board = chess.Board()
        for ind, move_data in enumerate(one_game_moves):
            expected_move_num = ind + 1
            current_move_num = move_data.get('n_move')
            if not expected_move_num == current_move_num:
                precessed_games_data[link].append({'n_move':ind+1,'fen':'IrregularIndexing'})
                precessed_games_data[link].append({'n_move':ind+1.5,'fen':'IrregularIndexing'})
                fens_sequence_with_counters.append(None)
                continue
                
            n_move = current_move_num
            white_move_san = move_data.get('white_move')
            black_move_san = move_data.get('black_move')
            
            move_obj_white = board.parse_san(white_move_san)
            board.push(move_obj_white)
            current_fen = board.fen()
            
            _ = chess.Board(current_fen)
            fen_to_insert = simplify(current_fen)
            precessed_games_data[link].append({'n_move':ind+1,'fen':fen_to_insert.fen})
            fens_sequence_with_counters.append(fen_to_insert)
            
            move_obj_black = board.parse_san(black_move_san)
            board.push(move_obj_black)
            current_fen = board.fen()

            _ = chess.Board(current_fen)
            fen_to_insert = simplify(current_fen)
            precessed_games_data[link].append({'n_move':ind+1.5,'fen':fen_to_insert.fen})
            fens_sequence_with_counters.append(fen_to_insert)
    
    return precessed_games_data, [x for x in fens_sequence_with_counters if x!=None]


def simplify_fen_and_extract_counters_for_insert(raw_fen: str) -> MainFenCreateData:
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
        moves_counter=f"#{parts[4]}#{parts[5]}_"
    )

async def insert_to_main_fens(all_raw_fens_with_counters: list[tuple[str, dict]]):

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


async def get_fens_to_insert(game_links: list[str]) -> list[tuple[str, dict]]:
    all_fens_with_counters = []
    moves_data_batch = await get_all_moves_for_links_batch(game_links)
    precessed_games_data, fens_sequence_with_counters = generate_fens_for_single_game_moves(moves)

    return precessed_games_data, fens_sequence_with_counters


async def get_new_fens(fens_sequence_with_counters: list[tuple[str, dict]]) -> list[tuple[str, dict]]:

    current_fens = {item.fen for item in fens_sequence_with_counters}
    existing_fens_in_db_rows = await open_async_request(
        """
        SELECT fen FROM main_fen WHERE fen = ANY(:fens);
        """,
        params={"fens": list(current_fens)}
    )
    existing_fens_set = {row[0] for row in existing_fens_in_db_rows}
    valid_fens = list(current_fens - existing_fens_set)
    fens_to_insert = [x for x in fens_sequence_with_counters if x.fen in valid_fens]
    fens_to_update = existing_fens_set
    return fens_to_insert, fens_to_update
def insert_fens():
    pass

async def collect_fens_operations(n_games: int, username: str = 'lafareto') -> Dict[str, Any]:
    new_game_links = await get_game_links_by_username(username, n_games)
    if not new_game_links:
        return {"status": "no_new_games", "fens_collected": 0, "games_processed": 0, "duration_seconds": 0.0}
    moves_data_batch = await get_all_moves_for_links_batch(new_games)
    precessed_games_data, fens_sequence_with_counters = await generate_fens_for_single_game_moves(moves_data_batch)
    if len(fens_sequence_with_counters) == 0:
        return {"status": "process fens failed", "fens_collected": 0, "games_processed": 0, "duration_seconds": 0.0}
    
    await insert_to_main_fens(fens_sequence_with_counters)
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

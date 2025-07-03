import os
import chess
import chess.engine
import asyncio
import time
import psutil
import concurrent.futures
import json
import re
import logging
from sqlalchemy import text, select
import subprocess 
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Set
from database.database.engine import AsyncDBSession
import math
from constants import LC0_PATH, lc0_directory, LC0_WEIGHTS_FILE
from database.database.db_interface import DBInterface
from database.database.models import Fen, FromGame
from database.database.ask_db import open_async_request
from database.operations.models import FenCreateData, FromGameCreateData

max_workers = 1

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
    filtering out games that have already been processed.
    """
    try:
        print(f"Attempting to fetch {limit} new game links for user '{username}' from the database...")

        # Fetch game links directly, excluding those already in processed_game
        game_links_rows = await open_async_request(
            """
            SELECT g.link
            FROM game AS g
            LEFT JOIN processed_game AS pg ON g.link = pg.link
            WHERE (g.white = :username OR g.black = :username)
            AND pg.link IS NULL
            """,
            params={"username": username, "query_limit": limit}
        )
        new_links = [row[0] for row in game_links_rows]

        if not new_links:
            print(f"No new game links found in the database for user '{username}'.")
            return []

        print(f"Fetched {len(new_links)} new game links for user '{username}'.")
        if limit == 'all':
            return new_links
        else:
            return new_links[:limit]

    except Exception as e:
        print(f"Error fetching game links for user {username} from DB: {e}", exc_info=True)
        return []


async def get_all_moves_for_links_batch(game_links: list[str]) -> dict[str, list[dict]]:
    """
    Fetches game moves for a batch of game links from the database.
    Returns a dictionary mapping game link to its list of moves.
    """
    moves_data_batch = {x:[] for x in game_links}
    if not game_links:
        return {}

    print(f"Fetching moves for {len(game_links)} game links from the database...")
    
    game_moves_rows = await open_async_request(
        """
        SELECT link, n_move, white_move, black_move
        FROM moves
        WHERE link = ANY(:game_links);
        """,
        params={"game_links": game_links}, fetch_as_dict = True
    )

    # for link, n_move, white_move, black_move in game_moves_rows:
    #     moves_data_batch[link].append({
    #         'n_move': n_move,
    #         'white_move': white_move,
    #         'black_move': black_move
    #     })

    print(f"Finished fetching and structuring moves for {len(moves_data_batch)} games.")
    return game_moves_rows
async def insert_processed_games_data(processed_games_move_data):
    for ind, chunk in enumerate(processed_games_move_data):
        processed_game_moves_db = DBInterface(GameFen)
        chunk_data = processed_games_move_data[ind]
        to_insert = []
        for link in chunk_data.keys():
            for move in chunk_data[link]:
                to_insert.append({'link':int(link),'n_move':int(move['n_move']),'fen':move['fen']})
        valid_items = [GameFenCreateData(**x) for x in to_insert]
        valid_items = [x.model_dump() for x in valid_items]
        await processed_game_moves_db.create_all(valid_items)
    return 'DONE'

async def divide_moves_data_batch_into_chunks_of_batch_size(
    moves_data_batch: dict,
    chunk_size: int
) -> list[dict]:
    """
    Divides a dictionary (moves_data_batch) into a list of smaller dictionaries (chunks).
    Each chunk will contain up to `chunk_size` items (game_link: moves_list pairs).
    """
    items = list(moves_data_batch.items()) # Get all (key, value) pairs as a list
    total_items = len(items)
    num_chunks = math.ceil(total_items / chunk_size)
    
    chunks = []
    for i in range(0, total_items, chunk_size):
        chunk_items = items[i : i + chunk_size]
        chunks.append(dict(chunk_items)) # Convert the slice of items back to a dictionary
    return chunks

async def generate_fens_for_single_game_moves(moves_data_batch: dict[str, list]) -> tuple[dict[str, list[dict]], list[dict]]:
    """
    Generates FENs for a batch of games' moves in parallel using a ThreadPoolExecutor.
    Uses asyncio.gather for efficient collection of results.
    """
    processed_games_data = {}
    fens_sequence_with_counters = []

    # Helper function to process a single game's moves
    def process_single_game_sync(link: str, one_game_moves: list) -> tuple[str, list[dict], list[dict]]:
        """
        Synchronous helper function to be run in a separate thread.
        Processes moves for a single game and returns its data.
        """
        game_processed_data = []
        game_fens_with_counters = []
        board = chess.Board()

        for ind, move_data in enumerate(one_game_moves):
            expected_move_num = ind + 1
            current_move_num = move_data.get('n_move')

            if not expected_move_num == current_move_num:
                # If irregular, append placeholder data and skip FEN generation for these half-moves
                game_processed_data.append({'n_move': ind + 1, 'fen': 'IrregularIndexing'})
                game_processed_data.append({'n_move': ind + 1.5, 'fen': 'IrregularIndexing'})
                continue # Do not append FEN to game_fens_with_counters for irregular indexing

            white_move_san = move_data.get('white_move')
            black_move_san = move_data.get('black_move')

            try:
                # White's move
                move_obj_white = board.parse_san(white_move_san)
                board.push(move_obj_white)
                current_fen_white = board.fen()
                fen_to_insert_white = simplify_fen_and_extract_counters_for_insert(current_fen_white)
                game_processed_data.append({'n_move': ind + 1, 'fen': fen_to_insert_white.fen})
                game_fens_with_counters.append(fen_to_insert_white)

                # Black's move
                move_obj_black = board.parse_san(black_move_san)
                board.push(move_obj_black)
                current_fen_black = board.fen()
                fen_to_insert_black = simplify_fen_and_extract_counters_for_insert(current_fen_black)
                game_processed_data.append({'n_move': ind + 1.5, 'fen': fen_to_insert_black.fen})
                game_fens_with_counters.append(fen_to_insert_black)

            except Exception as e:
                #print(f"Error processing move in game {link}, move {current_move_num}: {e}")
                game_processed_data.append({'n_move': ind + 1, 'fen': 'InvalidMove'})
                game_processed_data.append({'n_move': ind + 1.5, 'fen': 'InvalidMove'})
                continue # Do not append FEN to game_fens_with_counters for invalid moves

        return link, game_processed_data, game_fens_with_counters

    # Get the current running event loop
    loop = asyncio.get_running_loop()

    # Create a ThreadPoolExecutor
    # You might want to explicitly set max_workers based on your CPU cores
    # For CPU-bound tasks, usually min(32, os.cpu_count() + 4) or just os.cpu_count()
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    #with concurrent.futures.ThreadPoolExecutor() as executor:
        # Create a list of awaitable futures for each game
        tasks = []
        for link, one_game_moves in moves_data_batch.items():
            tasks.append(
                loop.run_in_executor(executor, process_single_game_sync, link, one_game_moves)
            )

        # Use asyncio.gather to await all tasks concurrently
        # return_exceptions=True allows other tasks to complete even if one fails
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process the results
        for result in results:
            if isinstance(result, Exception):
                # Handle exceptions from individual tasks
                # The 'link' information is lost here, you might need to store it
                # with the task if you want to log it specifically.
                # For example, by wrapping the task in a lambda that captures the link
                # or by passing the link as part of the returned error info from the thread.
                # For now, just log the exception.
                print(f"A game processing task raised an exception: {result}", exc_info=True)
                # You might add a placeholder for the failed game in processed_games_data
                # e.g., processed_games_data["unknown_link_error"] = [{'n_move': 0, 'fen': 'ErrorProcessingGame'}]
            else:
                link, game_processed_data, game_fens_with_counters = result
                processed_games_data[link] = game_processed_data
                fens_sequence_with_counters.extend(game_fens_with_counters)

    # The original function filtered out None.
    # With the new structure, we ensure only valid fens are appended to game_fens_with_counters.
    # If a specific 'None' equivalent is needed for 'IrregularIndexing' in the fens_sequence_with_counters,
    # the process_single_game_sync function would need to return `None` or a special object
    # that is then filtered out here.
    final_fens_sequence = [x for x in fens_sequence_with_counters if x is not None]

    return processed_games_data, final_fens_sequence

    # Use ThreadPoolExecutor to run process_single_game for each game concurrently
    loop = asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {
            loop.run_in_executor(executor, process_single_game, link, moves_data_batch[link]): link
            for link in moves_data_batch.keys()
        }

        for future in concurrent.futures.as_completed(futures):
            original_link = futures[future]
            try:
                link, game_processed_data, game_fens_with_counters = await future
                processed_games_data[link] = game_processed_data
                fens_sequence_with_counters.extend(game_fens_with_counters)
            except Exception as exc:
                print(f'{original_link} generated an exception: {exc}')
                # Optionally, handle the error by adding error markers or skipping the game
                processed_games_data[original_link] = [{'n_move': 0, 'fen': 'ErrorProcessingGame'}]

    # The original function filtered out None.
    # With the new structure, we ensure only valid fens are appended to game_fens_with_counters.
    # If 'IrregularIndexing' still needs to result in 'None' in the fens_sequence_with_counters,
    # the process_single_game function would need to return `None` or a special object
    # that is then filtered out here.
    # Assuming 'IrregularIndexing' indicates a skipped FEN for the `fens_sequence_with_counters` list:
    final_fens_sequence = [x for x in fens_sequence_with_counters if x is not None]

    return processed_games_data, final_fens_sequence


def simplify_fen_and_extract_counters_for_insert(raw_fen: str) -> FenCreateData:
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




async def get_new_fens(fens_sequence_with_counters: list[tuple[str, dict]]) -> list[tuple[str, dict]]:

    current_fens = {item.fen for item in fens_sequence_with_counters}
    existing_fens_in_db_rows = await open_async_request(
        """
        SELECT fen FROM main_fen WHERE fen = ANY(:fens);
        """,
        params={"fens": list(current_fens)}
    )
    existing_fens_set = {row[0] for row in existing_fens_in_db_rows}
    valid_fens = current_fens - existing_fens_set
    fens_to_insert = [x for x in fens_sequence_with_counters if x.fen in valid_fens]
    fens_to_update = [x for x in fens_sequence_with_counters if x.fen in existing_fens_set]
    fens_to_insert = [x.model_dump() for x in fens_to_insert]
    fens_to_update = [x.model_dump() for x in fens_to_update]
    
    return fens_to_insert, fens_to_update

async def db_info():
    async def all_main_fens() -> str:
        """
        Fetches the count of FENs in 'main_fen' table where 'n_games' is not equal to 1,
        formatted with commas.
        """
        sql_query = "SELECT COUNT(fen) FROM main_fen;"
        result = await open_async_request(sql_query)
        count = result[0][0] if result else 0
        return f"RAW {count:,}"
    async def all_fens() -> str:
        sql_query = "SELECT COUNT(fen) FROM fen;"
        result = await open_async_request(sql_query)
        count = result[0][0] if result else 0
        return f"ANALYZED {count:,}"
    async def games_processed() -> str:
        sql_query = "SELECT COUNT(link) FROM processed_game;"
        result = await open_async_request(sql_query)
        count = result[0][0] if result else 0
        return f"PROCESSED {count:,}"
    # async def moves_for_games() -> str:
    #     sql_query = "SELECT COUNT(link) FROM game_fen;"
    #     result = await open_async_request(sql_query)
    #     count = result[0][0] if result else 0
    #     return f"MOVES {count:,}"
    print(await all_main_fens())
    print(await all_fens())
    print(await games_processed())
    #print(await moves_for_games())
def divide_into_batches(
                        fens_list_1: List[dict],
                        fens_list_2: List[dict],
                        db_batch_size: int = 5000 # A safe default for database inserts/updates
                    ) -> Tuple[List[List[dict]], List[List[dict]]]:
    """
    Divides two lists of FEN objects into smaller sub-lists (batches) for database operations.
    
    :param fens_list_1: The first list of FEN objects (e.g., objects_to_insert).
    :param fens_list_2: The second list of FEN objects (e.g., objects_to_update).
    :param db_batch_size: The maximum number of FEN objects per sub-batch.
                          This should be chosen to respect database parameter limits.
    :return: A tuple containing two lists of lists, representing the batched versions
             of fens_list_1 and fens_list_2.
    """
    batched_list_1 = []
    for i in range(0, len(fens_list_1), db_batch_size):
        batched_list_1.append(fens_list_1[i : i + db_batch_size])

    batched_list_2 = []
    for i in range(0, len(fens_list_2), db_batch_size):
        batched_list_2.append(fens_list_2[i : i + db_batch_size])
        
    return batched_list_1, batched_list_2
async def insert_processed_game_links(new_games):
    valid_structure = [ProcessedGameCreateData(**{'link':link,'analyzed':False})
                       for link in new_games]
    processed_game_db = DBInterface(ProcessedGame)
    await processed_game_db.create_all([x.model_dump() for x in valid_structure])
async def insert_fen_in_chunks(fen_insert, fen_update):
    main_fen_interface = DBInterface(MainFen)
    
    for update_item in range(len(fen_update)):
        #print(f'chunk_update {update_item} out of {len(fen_update)}')
        fen_up_chunk = fen_update[update_item]
        await main_fen_interface.upsert_main_fens([],fen_up_chunk)
        #print(f'COMPLETED')
        
    for insert_item in range(len(fen_insert)):
        #print(f'chunk_insert {insert_item} out of {len(fen_insert)}')
        fen_in_chunk = fen_insert[insert_item]
        await main_fen_interface.upsert_main_fens(fen_in_chunk,[])
        #print(f'COMPLETED')
        
    
        
async def insert_fens_and_get_moves(moves_data_batch):
    batch_size = 10000
    every_game = moves_data_batch
    
    processed_games_move_data = []
    fen_insert, fen_update = [],[]
    parts = await divide_moves_data_batch_into_chunks_of_batch_size(moves_data_batch,batch_size) 
    for ind, part in enumerate(parts): 
        start = time.time()
        a, b = await generate_fens_for_single_game_moves(part)
        processed_games_move_data.append(a)
        end = time.time()
        print(f'generate fens for insert part: {ind} of {len(parts)}',end-start)
        fens_to_insert, fens_to_update = await get_new_fens(b)
        
        fen_insert.extend(fens_to_insert)
        fen_update.extend(fens_to_update)
    
    parts_fens_to_insert, parts_fens_to_update = divide_into_batches(fen_insert, fen_update)
    return processed_games_move_data,parts_fens_to_insert, parts_fens_to_update
async def collect_fens_from_player(player_name: str) -> str:
    print('###################################')
    print('DB initial mesurements: ')
    await db_info()
    print('###################################')
    
    new_games = await get_game_links_by_username(player_name, "all")
    moves_data_batch = await get_all_moves_for_links_batch(new_games)
    a,b,c = await insert_fens_and_get_moves(moves_data_batch)
    processed_games_move_data,parts_fens_to_insert, parts_fens_to_update = a,b,c
    await insert_fen_in_chunks(parts_fens_to_insert, parts_fens_to_update)
    await insert_processed_game_links(new_games)
    await insert_processed_games_data(processed_games_move_data)
    
    print('DB FINAL mesurements: ')
    await db_info()
    return 'DONEEEEEEEEEEEEEEEEE'
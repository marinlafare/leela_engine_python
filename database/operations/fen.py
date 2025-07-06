# OPERATIONS_FEN
import os
import concurrent.futures
from collections import defaultdict

import chess
import chess.engine
import asyncio
import time
import psutil
import json
import re
import logging
import subprocess # Ensure subprocess is imported for get_system_metrics
from typing import List, Dict, Any
from constants import LC0_PATH, lc0_directory, LC0_WEIGHTS_FILE
from database.operations.models import FenCreateData, FenGameAssociateData
from database.database.db_interface import DBInterface
from database.database.models import Fen, Game
from database.database.ask_db import open_async_request, get_game_links_by_username
# from database.operations.collect_data import (generate_fens_for_single_game_moves,
#                                                 get_all_moves_for_links_batch,
#                                                 insert_processed_game_links, simplify_fen_and_extract_counters_for_insert)


async def initialize_lc0_engine() -> chess.engine.UciProtocol:
    """
    Launches and configures the Leela Chess Zero (Lc0) engine.
    Returns:
        An initialized Lc0 engine instance (chess.engine.UciProtocol).
    Raises:
        Exception: If the engine fails to launch or configure or something.
    """
    engine_uci = None
    try:
        print("...Starting Leela engine...")
        # Removed stderr=subprocess.DEVNULL to show all stderr output
        transport, engine_uci = await chess.engine.popen_uci(LC0_PATH, cwd=lc0_directory)
        await engine_uci.configure({
            "WeightsFile": LC0_WEIGHTS_FILE,
            "Backend": "cuda-fp16",
            "Threads": 1,
            "MinibatchSize": 1024
        })
        print("...Leela engine ready...")
        return engine_uci
    except Exception as e:
        print(f"######### Error initializing Lc0 engine: {e} #########")
        if engine_uci:
            await engine_uci.quit()
        raise


async def analize_fen(fen: str,nodes_limit:int = 50_000, time_limit: int = 2):
    engine = await initialize_lc0_engine()
    #analysis_data = await analyze_single_position(engine, fen, nodes_limit)
    board = chess.Board(fen)
    info = await engine.analyse(board, chess.engine.Limit(time=time_limit,nodes=nodes_limit))
    next_moves = ''.join([str(x)+'#' for x in info['pv']][:6])
    score = info['score'].white().cp / 100
        
    return score, next_moves
    

async def merge_fen_entries(fen_data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Merges a list of FEN entry dictionaries.
    Entries with the same 'fen' are combined by:
    - Summing their 'n_games' values.
    - Appending their 'moves_counter' strings, ensuring uniqueness.
    - Setting 'next_moves' and 'score' to None.

    Args:
        fen_data_list (List[Dict[str, Any]]): A list of dictionaries, where each
                                             dictionary represents a FEN entry.
                                             Expected keys: 'fen', 'n_games',
                                             'moves_counter', 'next_moves', 'score'.

    Returns:
        List[Dict[str, Any]]: A new list containing the merged FEN entries,
                              with one unique entry per 'fen'.
    """
    # Use a defaultdict to group entries by their 'fen' key
    # The value for each fen will be a list of all dictionaries with that fen
    grouped_by_fen = defaultdict(list)
    for entry in fen_data_list:
        if 'fen' in entry:
            grouped_by_fen[entry['fen']].append(entry)
        else:
            # Handle entries without a 'fen' key if necessary, or skip them
            print(f"Warning: Skipping entry without 'fen' key: {entry}")

    merged_results = []

    # Iterate through the grouped FENs and merge the entries
    for fen, entries in grouped_by_fen.items():
        if not entries:
            continue # Should not happen with defaultdict, but good for safety

        # Initialize the merged entry with the first item's structure,
        # but reset score and next_moves to None as per your SQL logic.
        merged_entry = {
            'fen': fen,
            'n_games': 0,
            'moves_counter': [], # Use a list to collect parts, then join
            'next_moves': None,
            'score': None
        }

        for entry in entries:
            # Sum n_games
            merged_entry['n_games'] += entry.get('n_games', 0)

            # Append moves_counter, ensuring it's a string, not empty, and not already present
            moves_counter_part = entry.get('moves_counter')
            if isinstance(moves_counter_part, str) and moves_counter_part:
                if moves_counter_part not in merged_entry['moves_counter']: # Added uniqueness check
                    merged_entry['moves_counter'].append(moves_counter_part)
            # If moves_counter is None or not a string, it's effectively skipped for appending.

            # For 'next_moves' and 'score', we are explicitly setting them to None
            # in the merged result, mirroring your SQL's ON CONFLICT DO UPDATE behavior.
            # If you wanted to keep the last non-None value, the logic would be different.
            # merged_entry['next_moves'] = entry.get('next_moves', None) if entry.get('next_moves') is not None else merged_entry['next_moves']
            # merged_entry['score'] = entry.get('score', None) if entry.get('score') is not None else merged_entry['score']

        # Join the collected moves_counter parts into a single string
        # Using '#' as a separator, similar to your example '#0#1_'
        merged_entry['moves_counter'] = ''.join(merged_entry['moves_counter'])

        merged_results.append(merged_entry)

    return merged_results
async def player_moves_to_analyze(player_name:str) -> str:
    moves_data_batch = defaultdict(list)
    
    sql_query = """SELECT
                    m.link,
                    m.n_move,
                    m.white_move,
                    m.black_move
                FROM
                    moves AS m
                INNER JOIN
                    game AS g ON m.link = g.link
                WHERE
                    g.fens_done = FALSE
                    AND (g.white = :username OR g.black = :username);"""
    
    
    
    result = await open_async_request(sql_query,
                                      params={"username": player_name},
                                      fetch_as_dict = True)
    for item in result:
        moves_data_batch[item.link].append({'n_move':item.n_move,
                                            'white_move':item.white_move,
                                            'black_move':item.black_move})
        
    return moves_data_batch #[x[0] for x in result]
    
async def get_batches(data_list: list, batches_size: int) -> list[list]:
    batches = []
    for i in range(0, len(data_list), batches_size):
        batches.append(data_list[i:i + batches_size])
    return batches
def process_single_game_sync(link: int, one_game_moves: list) -> list[dict]:
    """
    Synchronous helper function to be run in a separate thread.
    Processes moves for a single game and returns its FEN data.
    """
    fens_to_insert = []
    board = chess.Board()

    # Sort the moves by n_move to ensure correct processing order
    data = sorted(one_game_moves, key=lambda x: x['n_move'])

    try:
        for ind, move_data in enumerate(data):
            expected_move_num = ind + 1
            current_move_num = move_data.get('n_move')

            if not expected_move_num == current_move_num:
                #print(f'Warning: Move number mismatch for game link {link}. Expected {expected_move_num}, got {current_move_num}. Skipping game.')
                return [] # Return empty list if there's a mismatch

            white_move_san = move_data.get('white_move')
            black_move_san = move_data.get('black_move')

            # Validate and apply white's move
            try:
                move_obj_white = board.parse_san(white_move_san)
                board.push(move_obj_white)
                current_fen_white = board.fen()
                # Assuming simplify_fen is defined elsewhere and returns a dict
                # Restored the second argument for simplify_fen
                to_insert_white = simplify_fen(current_fen_white, ind, link)
                fens_to_insert.append(to_insert_white)
            except ValueError as e: # Catch specific chess move errors
                #print(f"Error parsing/applying white move '{white_move_san}' in game {link} at move {current_move_num}: {e}")
                return [] # Return empty list on error

            # Validate and apply black's move
            try:
                move_obj_black = board.parse_san(black_move_san)
                board.push(move_obj_black)
                current_fen_black = board.fen()
                # Assuming simplify_fen is defined elsewhere and returns a dict
                # Restored the second argument for simplify_fen
                to_insert_black = simplify_fen(current_fen_black, ind + 0.5, link)
                fens_to_insert.append(to_insert_black)
            except ValueError as e: # Catch specific chess move errors
                #print(f"Error parsing/applying black move '{black_move_san}' in game {link} at move {current_move_num}: {e}")
                return [] # Return empty list on error

    except Exception as e:
        # Catch any other unexpected errors during game processing
        #print(f"An unexpected error occurred while processing game {link}: {e}")
        return [] # Return empty list on any exception

    return fens_to_insert



def simplify_fen(raw_fen: str, n_move: float, link:int) -> FenCreateData:
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
    return {'link':link,
            'fen':simplified_fen,
            'n_games':1,
            'moves_counter':f"#{parts[4]}#{parts[5]}_",
            'n_move' : n_move,
            'next_moves' : None,
            'score' : None}
  
async def create_fens_from_games_dict(moves: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """
    Asynchronously creates FEN dictionaries from a dictionary of game moves
    by parallelizing the processing of individual games using a ProcessPoolExecutor.

    Args:
        moves (Dict[str, List[Dict[str, Any]]]): A dictionary where keys are game links
                                                 and values are lists of move dictionaries.

    Returns:
        List[Dict[str, Any]]: A flattened list of all FEN dictionaries generated from all games.
    """
    print('Starting create_fens_from_games_dict (using ProcessPoolExecutor)')
    result_fens = []
    loop = asyncio.get_running_loop()

    # Determine the number of processes to use.
    # It's good practice to cap this to avoid overwhelming the system,
    # even if os.cpu_count() returns a very high number.
    # num_processes = os.cpu_count() or 1
    # if num_processes > 8: # Arbitrary cap, adjust based on your system and workload
    #     num_processes = 8
    num_processes = 12
    print('LEN MOVES:::::', len(moves))
    # Using ProcessPoolExecutor for CPU-bound tasks
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_processes) as executor:
        tasks = []
        for link, one_game_moves in moves.items():
            # Submit each game's processing as a separate task to the process pool
            tasks.append(
                # run_in_executor is used to bridge async code with synchronous blocking calls
                loop.run_in_executor(executor,
                                     process_single_game_sync,
                                     link, one_game_moves)
            )
        
        # Await all tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process the results from each worker process
        for result in results:
            if isinstance(result, Exception):
                continue
            elif isinstance(result, list): # Expecting a list of FEN dicts
                result_fens.extend(result)
            else:
                print(f"Unexpected result type from worker: {type(result)} - {result}")

    print('End of create_fens_from_games_dict')
    print('FENS LEN:::::: ', len(result_fens))
    return result_fens


        
async def insert_fens_from_player(player_name, game_batches = 1000):
    fen_interface = DBInterface(Fen)
    #link_interface = DBInterface(FromGame)
    
    start_getting_moves = time.time()
    moves = await player_moves_to_analyze(player_name)
    if len(moves) == 0:
        return 'NO NEW GAMES'
    end_getting_moves = time.time()
    print('MOVES LEN: ',len(moves))
    print('getting_moves in: ',end_getting_moves - start_getting_moves)
    start_create_fens = time.time()
    fens = await create_fens_from_games_dict(moves)
    end_create_fens = time.time()
    print('created_fens in: ', end_create_fens - start_create_fens)
    to_insert_associations = [{'game_link':x.pop('link'), 'fen_fen':x['fen']} for x in fens]
    start_merge_fens = time.time()
    fens = await merge_fen_entries(fens)
    end_merge_fens = time.time()
    print('merged_fens in: ', end_merge_fens - start_merge_fens)
    start_insert_fens = time.time()
    await fen_interface.create_all(fens)
    end_insert_fens = time.time()
    print('inserted_fens in ', end_insert_fens - start_insert_fens)
    start_validate_and_insert_associations = time.time()
    #validate_to_insert_associations = [FenGameAssociateData(**x) for x in to_insert_associations]
    await DBInterface(Fen).associate_fen_with_games(to_insert_associations)
    end_validation_and_associations_insert = time.time()

    print('validation done in: ',end_validation_and_associations_insert-start_validate_and_insert_associations)
    game_interface = DBInterface(Game)
    start_game_associations = time.time()
    links_with_extracted_fens = [x['game_link'] for x in to_insert_associations]
    await game_interface.update_all(links_with_extracted_fens)
    end_game_associations = time.time()
    print('insertion done in: ',end_game_associations-start_game_associations)
    return 'DONEEEEEEEEEEEEEEEEEEEEE'


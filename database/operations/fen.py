# database.operations.fen.py

import os
import psutil
import json
import re
import logging
import subprocess
import concurrent.futures
from collections import defaultdict
import sys
import glob
from database.database.ask_db import open_async_request
from database.database.db_interface import DBInterface
from database.database.models import Fen, Game
import time
import chess
import chess.engine
import asyncio

from typing import List, Dict, Any
from constants import * # Assuming constants like BACKEND_DEFAULT, GPU_DEFAULT, BACKEND_ALTERNATIVE, GPU_ALTERNATIVE are defined here

# LC0_PATH and LC0_DIRECTORY will now be passed as arguments to functions that need them.
# They are no longer expected to be global within this file.

async def get_weights_list(lc0_directory: str):
    """
    Retrieves a list of weight files from the specified Lc0 directory.
    """
    weights_list = [x for x in os.listdir(lc0_directory) if x.endswith('.pb.gz')]
    return weights_list

async def initialize_lc0_engine(
                WEIGHTS: str = "t1-256x10-distilled-swa-2432500.pb.gz"
            ) -> chess.engine.UciProtocol:
    """
    Initializes and configures the Lc0 engine.
    """
    sys.stdout.flush()
    engine_uci = None
    try:
        print("...Starting Leela engine...")
        transport, engine_uci = await chess.engine.popen_uci(LC0_PATH, cwd=LC0_DIRECTORY)
        print("Lc0 engine loaded successfully.")

        await engine_uci.configure({
            "WeightsFile": WEIGHTS,
            "Backend": BACKEND_DEFAULT, # Use the passed backend_default
            "BackendOptions": f"gpu={GPU_DEFAULT}", # Use the passed gpu_default
            "Threads": 1,
            "MinibatchSize": 1024
        })
        print("Engine configuration sent.")
        print("...Leela engine ready...")
        return engine_uci
    except Exception as e:
        print(f"######### Error initializing Lc0 engine yo!: {e} #########")
        if engine_uci:
            await engine_uci.quit()
        raise

async def alternative_initialize_lc0_engine(
                                        WEIGHTS: str = "t1-256x10-distilled-swa-2432500.pb.gz"
                                    ) -> chess.engine.UciProtocol:
    """
    Initializes and configures the Lc0 engine for an alternative backend/GPU.
    """
    sys.stdout.flush()
    engine_uci = None
    try:
        print("...Starting Leela engine...")
        transport, engine_uci = await chess.engine.popen_uci(LC0_PATH, cwd=LC0_DIRECTORY)
        print("Lc0 engine loaded successfully.")

        await engine_uci.configure({
            "WeightsFile": WEIGHTS,
            "Backend": BACKEND_ALTERNATIVE, # Use the passed alternative backend
            "BackendOptions": f"gpu={GPU_ALTERNATIVE}", # Use the passed alternative gpu_id
            "Threads": 1,
            "MinibatchSize": 1024
        })
        print("Engine configuration sent (Alternative Backend).")
        print("...Leela engine ready (Alternative Backend)...")
        return engine_uci
    except Exception as e:
        print(f"######### Error initializing Lc0 engine (Alternative Backend): {e} #########")
        if engine_uci:
            await engine_uci.quit()
        raise

async def analize_fen(
    fen: str,
    nodes_limit: int = 50_000,
    time_limit: int = 2,
    weights: str = "t1-256x10-distilled-swa-2432500.pb.gz",
):
    """
    Analyzes a single FEN position using the Lc0 engine.
    """
    engine = None # Initialize engine to None
    try:
        # Pass all necessary configuration to initialize_lc0_engine
        engine = await initialize_lc0_engine(WEIGHTS = weights)
        board = chess.Board(fen)
        info = await engine.analyse(board, chess.engine.Limit(time=time_limit,nodes=nodes_limit))
        next_moves = ''.join([str(x)+'#' for x in info['pv']][:6])
        score = info['score'].white().cp / 100
        return score, next_moves
    except Exception as e:
        print(f"An error occurred during analize_fen: {e}")
        raise # Re-raise the exception to see full traceback in Jupyter
    finally:
        if engine:
            print("\nQuitting Lc0 engine...")
            await engine.quit()
            print("Lc0 engine quit.")

async def analize_most_repeated_fens(
                            between: tuple = (5,10),
                            limit: int = 100,
                            verbose_each: int = 100,
                            analyse_time_limit: float = 1.0,
                            nodes_limit: int = 50_000,
                            weights:str ="t1-256x10-distilled-swa-2432500.pb.gz",
                            card:int = 0
                        ):
    """
    Analyzes a batch of most repeated FENs using the default Lc0 engine configuration.
    """
    
    get_batch_of_repeated = await get_repeated_fens_between(between, limit)
    # Pass all necessary configuration to initialize_lc0_engine
    if card == 0:
        leela_engine = await initialize_lc0_engine(WEIGHTS = weights)
    else:
        leela_engine = await alternative_initialize_lc0_engine(WEIGHTS = weights)
    results = await analyse_fens_with_engine(
                        get_batch_of_repeated,
                        leela_engine,
                        time_limit = analyse_time_limit,
                        nodes_limit = nodes_limit,
                        verbose_each = verbose_each,
                    )
    return results


# --- Other functions from your fen.py (unchanged by this modification) ---
async def format_leela_results(analize_results):
    to_insert = []
    for fen in analize_results.keys():
        next_moves = '#'.join([str(x)+'#' for x in analize_results[fen]['pv']][:8])\
                             .replace('+','').replace('-','')
        board = chess.Board(fen)
        if board.is_checkmate():
            if board.turn == chess.BLACK:
                to_insert.append({'fen':fen,
                                  'score':1000,
                                 'next_moves':'mate'})
            else:
                to_insert.append({'fen':fen,
                                  'score':-1000,
                                 'next_moves':'mate'})
        else:
            try:
                to_insert.append({'fen':fen,
                                  'score':int(str(analize_results[fen]['score'].relative).replace('+','').replace('-','').replace('#',''))/100,
                                 'next_moves':next_moves})
            except:
                to_insert.append({'fen':fen,
                                  'score':0,
                                 'next_moves':next_moves})
    return to_insert

# async def analize_most_repeated_fens_old(between:int = (5,10),
#                                          limit:int = 100,
#                                          verbose_each:int = 100,
#                                          analyse_time_limit:float = 1.0,
#                                          nodes_limit:int = 50_000):
#     # This function is now deprecated or needs to be updated to pass LC0_PATH etc.
#     # It's kept here as it was in your original file, but the new analize_most_repeated_fens
#     # above should be used.
#     get_batch_of_repeated = await get_repeated_fens_between(between, limit)
#     # This call will now fail if LC0_PATH is not defined in constants.py or passed.
#     # leela_engine = await initialize_lc0_engine()
#     # results = await analyse_fens_with_engine(get_batch_of_repeated,
#     #                                          leela_engine,
#     #                                          time_limit = analyse_time_limit,
#     #                                          nodes_limit = nodes_limit,
#     #                                          verbose_each = verbose_each)
#     # return results
#     pass # Placeholder if you want to keep the function definition but not use it.

def process_single_game_sync(link: int, one_game_moves: list) -> list[dict]:
    fens_to_insert = []
    board = chess.Board()
    data = sorted(one_game_moves, key=lambda x: x['n_move'])

    try:
        for ind, move_data in enumerate(data):
            expected_move_num = ind + 1
            current_move_num = move_data.get('n_move')

            if not expected_move_num == current_move_num:
                return []

            white_move_san = move_data.get('white_move')
            black_move_san = move_data.get('black_move')

            try:
                move_obj_white = board.parse_san(white_move_san)
                board.push(move_obj_white)
                current_fen_white = board.fen()
                to_insert_white = simplify_fen(current_fen_white, ind, link)
                fens_to_insert.append(to_insert_white)
            except ValueError as e:
                return []

            try:
                move_obj_black = board.parse_san(black_move_san)
                board.push(move_obj_black)
                current_fen_black = board.fen()
                to_insert_black = simplify_fen(current_fen_black, ind + 0.5, link)
                fens_to_insert.append(to_insert_black)
            except ValueError as e:
                return []

    except Exception as e:
        return []

    return fens_to_insert


def simplify_fen(raw_fen: str, n_move: float, link:int) -> Dict[str, Any]:
    parts = raw_fen.split(' ')
    simplified_fen = ' '.join(parts[:4])
    
    return {'link':link,
            'fen':simplified_fen,
            'n_games':1,
            'moves_counter':f"#{parts[4]}#{parts[5]}_",
            'n_move' : n_move,
            'next_moves' : None,
            'score' : None}
    
async def create_fens_from_games_dict(moves: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    print('Starting create_fens_from_games_dict (using ProcessPoolExecutor)')
    result_fens = []
    loop = asyncio.get_running_loop()
    num_processes = 12
    print('LEN MOVES:::::', len(moves))
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_processes) as executor:
        tasks = []
        for link, one_game_moves in moves.items():
            
            tasks.append(
                loop.run_in_executor(executor,
                                      process_single_game_sync,
                                      link, one_game_moves)
            )
        
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                continue
            elif isinstance(result, list):
                result_fens.extend(result)
            else:
                print(f"Unexpected result type from worker: {type(result)} - {result}")

    print('End of create_fens_from_games_dict')
    print('FENS LEN:::::: ', len(result_fens))
    return result_fens

async def get_repeated_fens_between(between: tuple = (5, 20), limit: int = 10):
    games_query = """
            SELECT
                fen
            FROM
                fen
            WHERE
                n_games BETWEEN :min_games AND :max_games
                AND score IS NULL
            ORDER BY
                n_games DESC
            LIMIT :limit;
            """
    
    # Extract min_games and max_games from the 'between' tuple
    min_games = between[0]
    max_games = between[1]

    raw = await open_async_request(
        games_query,
        params={
            "min_games": min_games,
            "max_games": max_games,
            "limit": limit
        },
        fetch_as_dict=True
    )
    return raw

async def get_repeated_fens(more_than:int = 5, limit:int = 100):
    games_query = """
            SELECT
                fen
            FROM
                fen
            WHERE
                n_games = :more_than AND score IS NULL
            ORDER BY
                n_games DESC
            LIMIT :limit;
            """
    raw = await open_async_request(games_query,
                                   params = {"more_than":more_than,"limit":limit},fetch_as_dict = True)
    return raw

async def analyse_fens_with_engine(
                                fens_list: list[str],
                                engine: chess.engine.UciProtocol,
                                time_limit: float = 2.0,
                                nodes_limit: int = 100_000,
                                verbose_each: int = 100,
                            ) -> dict[str, chess.engine.InfoDict]:
    fen_analysis_results: dict[str, chess.engine.InfoDict] = {}
    processed_count = 0
    total_fens = len(fens_list)

    current_engine = engine

    for fen_item in fens_list:
        fen_string = ""
        if hasattr(fen_item, 'fen'):
            fen_string = fen_item.fen
        elif isinstance(fen_item, dict) and 'fen' in fen_item:
            fen_string = fen_item['fen']
        elif isinstance(fen_item, str):
            fen_string = fen_item
        else:
            print(f"Skipping unknown type in fens_list: {type(fen_item)}. Expected string or object with 'fen' attribute/key.")
            sys.stdout.flush()
            processed_count += 1
            if processed_count % verbose_each == 0:
                print(f"{processed_count} out of {total_fens} FENs ready.")
                sys.stdout.flush()
            continue

        try:
            board = chess.Board(fen_string)

            # --- ADDED CHECKMATE/STALEMATE CHECK HERE ---
            if board.is_checkmate():
                print(f"FEN '{fen_string}' is a checkmate. Skipping engine analysis.")
                sys.stdout.flush()
                # Store a specific result for checkmate
                fen_analysis_results[fen_string] = {
                    "score": chess.engine.Cp(1000) if board.turn == chess.BLACK else chess.engine.Cp(-1000),
                    "pv": ['mate']
                }
                processed_count += 1
                if processed_count % verbose_each == 0:
                    print(f"{processed_count} out of {total_fens} FENs ready.")
                    sys.stdout.flush()
                continue
            elif board.is_stalemate():
                print(f"FEN '{fen_string}' is a stalemate. Skipping engine analysis.")
                sys.stdout.flush()
                # Store a specific result for stalemate (draw)
                fen_analysis_results[fen_string] = {"score": chess.engine.Cp(0), "pv": ['stalemate']}
                processed_count += 1
                if processed_count %100 == 0:
                    print(f"{processed_count} out of {total_fens} FENs ready.")
                    sys.stdout.flush()
                continue
            # --- END OF ADDED CHECK ---

            sys.stdout.flush()
            info = await current_engine.analyse(board, chess.engine.Limit(time=time_limit, nodes=nodes_limit))
            fen_analysis_results[fen_string] = info

        except ValueError as e:
            print(f"\nSkipping invalid FEN '{fen_string}': {e}")
            sys.stdout.flush()
            fen_analysis_results[fen_string] = {"error": f"Invalid FEN: {e}"}
            processed_count += 1
            if processed_count % verbose_each == 0:
                print(f"{processed_count} out of {total_fens} FENs ready.")
                sys.stdout.flush()
            continue
        except chess.engine.EngineError as e:
            print(f"\nEngine Error for FEN '{fen_string}': {e}")
            sys.stdout.flush()
            fen_analysis_results[fen_string] = {"error": f"Engine Error: {e}"}

            print(f"Attempting to quit and reinitialize the engine due to an error with FEN: {fen_string}...")
            sys.stdout.flush()
            if current_engine:
                try:
                    await current_engine.quit()
                    print("Previous engine instance quit successfully.")
                    sys.stdout.flush()
                except Exception as quit_e:
                    print(f"Error quitting previous engine instance: {quit_e}")
                    sys.stdout.flush()

        processed_count += 1
        if processed_count % verbose_each == 0:
                print(f"{processed_count} out of {total_fens} FENs ready.")
                sys.stdout.flush()
        sys.stdout.flush()

    if total_fens > 0:
        print("\n--- Analysis Complete ---")
        sys.stdout.flush()

    return fen_analysis_results

# async def analize_repeated_fens_operations(data:dict):
#     start_analysis_loop = time.time()
#     try:
        
#         gpu = data['gpu']
#         n_repetitions = int(data['n_repetitions'])
#         left_between = int(data['between'].split(',')[0])
#         right_between = int(data['between'].split(',')[1])
#         between = (left_between,right_between)
#         limit = int(data['limit'])
#         min_wait_time_seconds = int(data['min_seconds_wait'])
#         max_wait_time = 1/4 #quarter of the time it take to run one loop
#         verbose_each = int(data['verbose_each'])
#         analyse_time_limit = float(data['analyse_time_limit'])
#         nodes_limit = int(data['nodes_limit'])
#     except:
#         return "DATA BAD FORMATED OR SOMETHNG IDK"
#     for ind, batches in enumerate(range(n_repetitions)):
#         start_batch = time.time()
#         print(f'{batches} out of {n_repetitions}')
#         last_loop = list(range(n_repetitions))[-1]
#         if gpu == 'default':
#             # Pass LC0_PATH, LC0_DIRECTORY, BACKEND_DEFAULT, GPU_DEFAULT explicitly
#             analize_results = await analize_most_repeated_fens(
#                 between = between,
#                 limit= limit,
#                 verbose_each = verbose_each,
#                 analyse_time_limit = analyse_time_limit,
#                 nodes_limit = nodes_limit,
#                 lc0_path=LC0_PATH,
#                 lc0_directory=LC0_DIRECTORY,
#                 backend_default=BACKEND_DEFAULT,
#                 gpu_default=GPU_DEFAULT
#             )
#         else:
#             # Pass LC0_PATH, LC0_DIRECTORY, BACKEND_ALTERNATIVE, GPU_ALTERNATIVE explicitly
#             analize_results = await analize_most_repeated_fen_alternative_card(
#                 between = between,
#                 limit= limit,
#                 verbose_each = verbose_each,
#                 analyse_time_limit = analyse_time_limit,
#                 nodes_limit = nodes_limit,
#                 lc0_path=LC0_PATH,
#                 lc0_directory=LC0_DIRECTORY,
#                 backend_alternative=BACKEND_ALTERNATIVE,
#                 gpu_alternative=GPU_ALTERNATIVE
#             )
            
#         to_insert = await format_leela_results(analize_results)
#         await DBInterface(Fen).update_fen_analysis_data(to_insert)
    
#         print(f'{ind} batch done in: ', time.time()-start_batch)
#         print('#####')
#         print('... Cool of waiting ...')
#         print('#####')
#         if batches == last_loop:
#             print('Done everything in: ', time.time()-start_analysis_loop)
#             break
#         sleep_for = max(min_wait_time_seconds, (time.time()-start_batch) * max_wait_time)
#         print(f'... sleeping for {sleep_for} seconds')
#         time.sleep(sleep_for)
#         print('#####')
#         print('... End of waiting Cool ...')
#         print('#####')


# async def analize_most_repeated_fen_alternative_card_old(between:int = (5,20),
#                                                          limit:int = 100,
#                                                          verbose_each:int = 100,
#                                                          analyse_time_limit:float = 1.5,
#                                                          nodes_limit:int = 50_000):
#     # This function is now deprecated or needs to be updated to pass LC0_PATH etc.
#     # It's kept here as it was in your original file, but the new analize_most_repeated_fen_alternative_card
#     # above should be used.
#     get_batch_of_repeated = await get_repeated_fens_between(between, limit)
#     # leela_engine = await alternative_initialize_lc0_engine()
#     # results = await analyse_fens_with_engine(get_batch_of_repeated,
#     #                                          leela_engine,
#     #                                          time_limit = analyse_time_limit,
#     #                                          nodes_limit = nodes_limit,
#     #                                          verbose_each = verbose_each)
#     # return results
#     pass # Placeholder if you want to keep the function definition but not use it.

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
        
    return moves_data_batch
async def merge_fen_entries(fen_data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    
    grouped_by_fen = defaultdict(list)
    for entry in fen_data_list:
        if 'fen' in entry:
            grouped_by_fen[entry['fen']].append(entry)
        else:
            print(f"Warning: Skipping entry without 'fen' key: {entry}")

    merged_results = []

    for fen, entries in grouped_by_fen.items():
        if not entries:
            continue

        merged_entry = {
            'fen': fen,
            'n_games': 0,
            'moves_counter': [],
            'next_moves': None,
            'score': None
        }

        for entry in entries:
            
            merged_entry['n_games'] += entry.get('n_games', 0)
            moves_counter_part = entry.get('moves_counter')
            if isinstance(moves_counter_part, str) and moves_counter_part:
                if moves_counter_part not in merged_entry['moves_counter']:
                    merged_entry['moves_counter'].append(moves_counter_part)
        merged_entry['moves_counter'] = ''.join(merged_entry['moves_counter'])

        merged_results.append(merged_entry)

    return merged_results
async def gather_players_fens(player_name):
    player_name = player_name.lower()
    print(player_name)
    await insert_fens_from_player(player_name, game_batches = 1000)
    
async def insert_fens_from_player(player_name, game_batches = 1000):
    fen_interface = DBInterface(Fen)
    print(f'insert fens_from player: {player_name}')
    start_getting_moves = time.time()
    moves = await player_moves_to_analyze(player_name)
    if len(moves) == 0:
        return 'NO NEW GAMES'
        print('n moves: ',len(moves.keys()))
    end_getting_moves = time.time()
    print('N moves fetched: ',len(moves))
    print('time_elapsed: ',end_getting_moves - start_getting_moves)
    start_create_fens = time.time()
    fens = await create_fens_from_games_dict(moves)
    end_create_fens = time.time()
    print('ready created_fens in: ', end_create_fens - start_create_fens)
    to_insert_associations = [{'game_link':x.pop('link'), 'fen_fen':x['fen']} for x in fens]
    start_merge_fens = time.time()
    fens = await merge_fen_entries(fens)
    end_merge_fens = time.time()
    print('ready merged_fens in: ', end_merge_fens - start_merge_fens)
    start_insert_fens = time.time()
    await fen_interface.create_all(fens)
    end_insert_fens = time.time()
    print('ready fens_insertion in: ', end_insert_fens - start_insert_fens)
    start_validate_and_insert_associations = time.time()
    await DBInterface(Fen).associate_fen_with_games(to_insert_associations)
    end_validation_and_associations_insert = time.time()
    print('ready association_fen/game.link done in: ',end_validation_and_associations_insert-start_validate_and_insert_associations)
    game_interface = DBInterface(Game)
    start_game_associations = time.time()
    links_with_extracted_fens = [x['game_link'] for x in to_insert_associations]
    await game_interface.update_all(links_with_extracted_fens)
    end_game_associations = time.time()
    print('Updating game.fens_done in: ',end_game_associations-start_game_associations)
    return 'DONEEEEEEEEEEEEEEEEEEEEE'
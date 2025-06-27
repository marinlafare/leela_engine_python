# OPERATIONS_COLLECT_FENS
import time
import chess
from collections import defaultdict
from typing import Any, Dict, List
import asyncio
from itertools import chain
from fastapi import WebSocketDisconnect

# Ensure these imports are correct for your structure
from database.operations.connection import Connection # If still used for WebSockets
# IMPORTANT: Now import the async versions from ask_db
from database.database.ask_db import open_async_request, get_one_game, get_new_games_links # Changed import
from database.operations.models import MainFenCreateData, ProcessedGameCreateData
from database.database.db_interface import DBInterface # This now points to your async DBInterface
from database.database.models import MainFen, Fen, ProcessedGame # Ensure all models are imported
from sqlalchemy.orm import Session # For type hinting synchronous session (if any)
from database.database.engine import AsyncDBSession # Import AsyncDBSession for internal session management
from sqlalchemy import select # Ensure select is imported for ORM queries

# Add a BATCH_SIZE constant for querying existing FENs
EXISTING_FENS_QUERY_BATCH_SIZE = 10000 # Adjust this based on your database and typical list sizes

# --- Helper function: Generates FENs for a single game's moves ---
def generate_fens_for_single_game_moves(moves: list[dict]) -> list[str]:
    """
    Generates a sequence of FENs for a single chess game given its moves.
    Includes validation for the resulting FENs.
    Returns an empty list if an invalid move or FEN is encountered.

    Args:
        moves (list[dict]): A list of dictionaries, where each dictionary represents
                            a move with keys 'n_move', 'white_move', 'black_move'.

    Returns:
        list[str]: A list of valid FEN strings representing the board state after each half-move.
                   Returns an empty list if an invalid move or malformed FEN is found.
    """
    board = chess.Board()
    fens_sequence = []
    
    for ind, move in enumerate(moves):
        # Basic assertion to check move order consistency,
        # There are some games that begin in a random move, we don't want those
        if move['n_move'] != ind + 1:
            return [] # Changed from False to []
            
        n_move = move['n_move']
        white_move_san = move.get('white_move') # Use .get() for safer access
        black_move_san = move.get('black_move') # Use .get() for safer access

        # Apply White's move and get FEN
        if white_move_san: # Only attempt if white_move exists
            try:
                move_obj_white = board.parse_san(white_move_san)
                board.push(move_obj_white)
                current_fen = board.fen()
                # --- FEN VALIDATION AT GENERATION ---
                try:
                    # Attempt to create a board from the generated FEN to validate it
                    _ = chess.Board(current_fen) 
                    fens_sequence.append(current_fen)
                except ValueError as e:
                    # If the generated FEN itself is invalid, skip this game's FENs
                    # print(f"Invalid FEN generated for game link {move.get('link') if 'link' in move else 'unknown'} at move {n_move} (White): {current_fen} - {e}")
                    return [] 
            except (ValueError, chess.InvalidMoveError) as e:
                # print(f"Error applying White's move '{white_move_san}' at move number {n_move}: {e}")
                return [] # Return empty list if an invalid move is found
        
        # Apply Black's move and get FEN (only if black_move exists and white's move was successful)
        if black_move_san: # Only attempt if black_move exists
            try:
                move_obj_black = board.parse_san(black_move_san)
                board.push(move_obj_black)
                current_fen = board.fen()
                # --- FEN VALIDATION AT GENERATION ---
                try:
                    # Attempt to create a board from the generated FEN to validate it
                    _ = chess.Board(current_fen)
                    fens_sequence.append(current_fen)
                except ValueError as e:
                    # If the generated FEN itself is invalid, skip this game's FENs
                    # print(f"Invalid FEN generated for game link {move.get('link') if 'link' in move else 'unknown'} at move {n_move} (Black): {current_fen} - {e}")
                    return []
            except (ValueError, chess.InvalidMoveError) as e:
                # print(f"Error applying Black's move '{black_move_san}' at move number {n_move}: {e}")
                return [] # Return empty list if an invalid move is found
                
    return fens_sequence

# --- Optimized Database Fetching Function (NOW ASYNC) ---
async def get_all_moves_for_links_batch(game_links: List[int]) -> Dict[int, list[dict]]: # Changed type hint for game_links, and return dict key
    """
    Fetches all moves for a given list of game links in a single batched query
    to the database asynchronously.
    """
    if not game_links:
        return {}

    # CORRECTED SQL SYNTAX: Explicitly cast the bound parameter as a bigint array.
    # This helps asyncpg understand the type without syntax errors.
    sql_query = """
    SELECT link, n_move, white_move, black_move
    FROM moves
    WHERE link = ANY(CAST(:game_links AS bigint[]))
    ORDER BY link, n_move;
    """
    # Use open_async_request and await it
    result_tuples = await open_async_request(sql_query, params={"game_links": game_links})

    grouped_moves = defaultdict(list)
    for row in result_tuples:
        link, n_move, white_move, black_move = row
        grouped_moves[link].append({
            'n_move': n_move,
            'white_move': white_move,
            'black_move': black_move
        })
    return grouped_moves

# --- Main FEN Generation Orchestrator (NOW ASYNC) ---
async def get_fens_from_games_optimized(new_game_links_data: list[tuple]) -> list[str]:
    """
    Retrieves and generates unique FENs for a list of game links asynchronously.
    """
    all_fens = set()
    game_links_only = [x[0] for x in new_game_links_data]

    start_db_fetch = time.time()
    # Fetch all moves for all games in a single batched query - NOW AWAITING
    all_game_moves_grouped = await get_all_moves_for_links_batch(game_links_only)
    db_fetch_time = time.time() - start_db_fetch
    # print(f"Time to fetch all game moves from DB (batched): {db_fetch_time:.4f} seconds")

    total_fen_generation_time = 0
    games_processed = 0
    
    for game_link in game_links_only:
        game_moves = all_game_moves_grouped.get(game_link)
        
        if game_moves:
            fen_gen_start = time.time()
            try:
                game_fens = generate_fens_for_single_game_moves(game_moves)
                all_fens.update(game_fens) # Add FENs to the set
                games_processed += 1
            except Exception as e: # Catch any unexpected errors during FEN generation
                print(f"An unexpected error occurred while processing game {game_link}: {e}")
                # Continue to the next game even if one game fails
            total_fen_generation_time += (time.time() - fen_gen_start)
        else:
            # print(f"No moves found in the database for game link: {game_link}")
            pass # Suppress print for now

    if games_processed > 0:
        # print(f"Mean FEN generation time per game (excluding DB fetch): {total_fen_generation_time / games_processed:.4f} seconds")
        pass # Suppress print for now
        
    return list(all_fens) # Convert the set back to a list for the final output


# This function now calls the (now async) open_async_request
async def get_new_fens(posible_fens: list[str]) -> list[dict]: # Corrected return type hint
    """
    Compares a list of possible FENs against known FENs in the 'main_fen' table
    and returns only the FENs that are not already present at the DB.

    Args:
        posible_fens (list[str]): A list of FEN strings to check.

    Returns:
        list[dict]: A list of dictionaries, where each dictionary contains
                    'fen', 'n_games', and 'moves_counter' for new FENs.
    """
    if not posible_fens:
        return []

    # CORRECTED SQL SYNTAX: Explicitly cast the bound parameter as a text array.
    # This helps asyncpg understand the type without syntax errors.
    sql_query = """
    SELECT p_fen.f
    FROM UNNEST(CAST(:posible_fens AS text[])) AS p_fen(f)
    LEFT JOIN main_fen AS rf ON p_fen.f = rf.fen
    WHERE rf.fen IS NULL;
    """
    # Use open_async_request and await it
    result_tuples = await open_async_request(sql_query, params={"posible_fens": posible_fens})
    new_raw_fens = list(chain.from_iterable(result_tuples))

    new_fens_data = [simplify_fen_and_extract_counters_for_insert(f) for f in new_raw_fens]
    
    return new_fens_data

# --- Functions for Inserting Data (Adjusted for your DBInterface) ---
# This function is already async and its internal calls are handled
async def insert_fens(fens_raw_from_games: List[Dict[str, Any]]):
    """
    Aggregates incoming FENs (handling duplicates within the batch),
    then separates them into existing and new for bulk updates and inserts.
    """
    if not fens_raw_from_games:
        print("No FENs to insert or update.")
        return

    # --- NEW STEP 0: Aggregate duplicate FENs within the incoming batch ---
    aggregated_fens: Dict[str, Dict[str, Any]] = {}
    for fen_data in fens_raw_from_games:
        fen_str = fen_data['fen']
        moves_counter = fen_data['moves_counter']

        if fen_str not in aggregated_fens:
            aggregated_fens[fen_str] = {
                'fen': fen_str,
                'n_games_in_batch': 1,
                'moves_counter_in_batch': moves_counter
            }
        else:
            aggregated_fens[fen_str]['n_games_in_batch'] += 1
            
            current_batch_moves = aggregated_fens[fen_str]['moves_counter_in_batch'].split('#') if aggregated_fens[fen_str]['moves_counter_in_batch'] else []
            new_moves_from_current_occurrence = moves_counter.split('#') if moves_counter else []
            
            for move_part in new_moves_from_current_occurrence:
                if move_part and move_part not in current_batch_moves:
                    current_batch_moves.append(move_part)
            
            aggregated_fens[fen_str]['moves_counter_in_batch'] = '#'.join(current_batch_moves)

    fens_to_process = list(aggregated_fens.values())
    print(f"Aggregated {len(fens_raw_from_games)} raw FENs down to {len(fens_to_process)} unique FENs for processing.")

    incoming_fen_strings = [d['fen'] for d in fens_to_process]
    
    async with AsyncDBSession() as session:
        try:
            existing_fens_map = {}

            for i in range(0, len(incoming_fen_strings), EXISTING_FENS_QUERY_BATCH_SIZE):
                batch_fen_strings = incoming_fen_strings[i : i + EXISTING_FENS_QUERY_BATCH_SIZE]
                
                batch_existing_records = (await session.execute(
                    select(MainFen.fen, MainFen.n_games, MainFen.moves_counter)
                    .filter(MainFen.fen.in_(batch_fen_strings))
                )).all()

                for rec in batch_existing_records:
                    existing_fens_map[rec.fen] = {
                        'n_games': rec.n_games,
                        'moves_counter': rec.moves_counter
                    }
            
            print(f"Identified {len(existing_fens_map)} FENs already in the database after all batches.")
            if existing_fens_map:
                pass

            fens_for_insert_batch = []
            fens_for_update_batch = []

            for processed_fen_data in fens_to_process:
                fen_str = processed_fen_data['fen']
                batch_n_games = processed_fen_data['n_games_in_batch']
                batch_moves_counter = processed_fen_data['moves_counter_in_batch']

                if fen_str in existing_fens_map:
                    existing_db_data = existing_fens_map[fen_str]
                    
                    updated_n_games = existing_db_data['n_games'] + batch_n_games

                    current_db_moves_list = existing_db_data['moves_counter'].split('#') if existing_db_data['moves_counter'] else []
                    current_batch_moves_list = batch_moves_counter.split('#') if batch_moves_counter else []
                    
                    for move_part in current_batch_moves_list:
                        if move_part and move_part not in current_batch_moves_list: # Check if move_part already in list
                            current_db_moves_list.append(move_part)
                    
                    updated_moves_counter_str = '#'.join(current_db_moves_list)

                    fens_for_update_batch.append({
                        'fen': fen_str,
                        'n_games': updated_n_games,
                        'moves_counter': updated_moves_counter_str
                    })
                else:
                    fens_for_insert_batch.append(
                        MainFenCreateData(
                            fen=fen_str,
                            n_games=batch_n_games,
                            moves_counter=batch_moves_counter
                        ).model_dump()
                    )
            
            if fens_for_insert_batch:
                try:
                    await session.run_sync(
                        lambda sync_session: sync_session.bulk_insert_mappings(MainFen, fens_for_insert_batch)
                    )
                    print(f"Successfully bulk inserted {len(fens_for_insert_batch)} new FENs.")
                except Exception as e:
                    print(f"Error during bulk insert of new FENs: {e}")
                    raise

            if fens_for_update_batch:
                try:
                    await session.run_sync(
                        lambda sync_session: sync_session.bulk_update_mappings(MainFen, fens_for_update_batch)
                    )
                    print(f"Successfully bulk updated {len(fens_for_update_batch)} existing FENs.")
                except Exception as e:
                    print(f"Error during bulk update of existing FENs: {e}")
                    raise
            
            await session.commit()
            print("FEN insertion/update process complete.")

        except Exception as e:
            await session.rollback()
            print(f"An unexpected error occurred during FEN processing: {e}")
            raise

# This function is already async
async def insert_processed_game_links(links: list[tuple]):
    """
    Inserts a list of game links into the processed_game_links table asynchronously.
    """
    if not links:
        print("No game links to insert into processed_game table.")
        return

    try:
        to_insert_data = [{'link': x[0]} for x in links]
        processed_links_data = [ProcessedGameCreateData(**data).model_dump() for data in to_insert_data]

        processed_link_interface = DBInterface(ProcessedGame)
        await processed_link_interface.create_all(processed_links_data) # Await create_all
        print(f"Successfully inserted {len(links)} game links into processed_game.")
    except Exception as e:
        print(f"Error inserting game links into processed_game: {e}")
        raise

# This function is already async, but its internal calls need await
async def collect_fens_operations(n_games):
    """
    Orchestrates the collection of FENs from new games, their analysis,
    and insertion into the database asynchronously.
    """
    # get_new_games_links is now async and needs await
    new_game_links = await get_new_games_links(n_games)
    
    start_total_fen_gen = time.time()
    # get_fens_from_games_optimized is now async and needs await
    fen_set_from_games = await get_fens_from_games_optimized(new_game_links)
    # print(f'{len(fen_set_from_games)} fens from {len(new_game_links)}',time.time()-start_total_fen_gen)

    start_new_fens_check = time.time()
    # get_new_fens is now async and needs await
    new_fens = await get_new_fens(fen_set_from_games)
    # print(f'{len(new_fens)} fens','time elapsed: ',time.time()-start_new_fens_check)
    
    print("\n--- Inserting data into the database ---")
    start_insert_fens = time.time()
    await insert_fens(new_fens) # Await the async insert_fens
    print('insert_fens time elapsed: ', time.time() - start_insert_fens)
    
    start_insert_games = time.time()
    await insert_processed_game_links(new_game_links) # Await the async insert_processed_game_links
    print('insert_games time elapsed: ', time.time() - start_insert_games)

    return f"{len(new_fens)} NEW FENS from {len(new_game_links)} NEW GAMES"

def simplify_fen_and_extract_counters_for_insert(full_fen: str) -> dict: # Changed return type hint
    """
    Takes a full FEN string, extracts the halfmove clock and fullmove number,
    and returns the simplified FEN along with the extracted counters as a string.

    Args:
        full_fen (str): The complete FEN string (e.g., 'rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1').

    Returns:
        dict: A dictionary containing:
            - "fen": The simplified FEN string.
            - "n_games": Defaulted to 1 for initial insertion.
            - "moves_counter": A string containing the halfmove clock and fullmove number, separated by '#'.
    """
    parts = full_fen.split(' ')

    simplified_fen = full_fen # Default in case of malformed FEN
    extracted_counters = ""

    if len(parts) >= 4:
        simplified_fen_parts = parts[0:4]
        simplified_fen = ' '.join(simplified_fen_parts)

        if len(parts) >= 6:
            halfmove_clock = parts[4]
            fullmove_number = parts[5]
            extracted_counters = f"{halfmove_clock}#{fullmove_number}" # Remove leading/trailing '#' to be cleaner
        else:
            # print(f"Warning: FEN '{full_fen}' does not contain expected halfmove or fullmove numbers.")
            pass # Suppress print
    else:
        # print(f"Error: Malformed FEN string provided: '{full_fen}'. Expected at least 4 parts.")
        pass # Suppress print

    return {"fen":simplified_fen, "n_games": 1, "moves_counter":extracted_counters}

# # OPERATIONS_COLLECT_FENS
# import time
# import chess
# from collections import defaultdict
# from typing import Any, Dict, List
# import asyncio
# from itertools import chain
# from fastapi import WebSocketDisconnect

# from database.operations.connection import Connection
# from database.database.ask_db import open_async_request, get_one_game, get_new_games_links
# from database.operations.models import MainFenCreateData, ProcessedGameCreateData
# from database.database.db_interface import DBInterface
# from database.database.models import MainFen, Fen, ProcessedGame
# from sqlalchemy.orm import Session
# from database.database.engine import AsyncDBSession
# from sqlalchemy import select


# EXISTING_FENS_QUERY_BATCH_SIZE = 10000

# # --- Helper function: Generates FENs for a single game's moves ---
# def generate_fens_for_single_game_moves(moves: list[dict]) -> list[str]:
#     """
#     Generates a sequence of FENs for a single chess game given its moves.
#     Returns an empty list if an invalid move is encountered.

#     Args:
#         moves (list[dict]): A list of dictionaries, where each dictionary represents
#                             a move with keys 'n_move', 'white_move', 'black_move'.

#     Returns:
#         list[str]: A list of FEN strings representing the board state after each half-move.
#                    Returns an empty list if an invalid move is encountered.
#     """
#     board = chess.Board()
#     fens_sequence = []
    
#     for ind, move in enumerate(moves):
#         # Basic assertion to check move order consistency,
#         # There are some games that begin in a random move, we don't want those
#         if move['n_move'] != ind + 1:
#             # print(f"Warning: n_move mismatch for move {move['n_move']} at index {ind}. Expected {ind + 1}. "
#             #       f"Processing might be out of order for this game. Returning empty list.")
#             return []
            
#         n_move = move['n_move']
#         white_move_san = move.get('white_move')
#         black_move_san = move.get('black_move')

#         # Apply White's move and get FEN
#         if white_move_san:
#             try:
#                 move_obj_white = board.parse_san(white_move_san)
#                 board.push(move_obj_white)
#                 fens_sequence.append(board.fen())
#             except (ValueError, chess.InvalidMoveError) as e:
#                 # print(f"Error applying White's move '{white_move_san}' at move number {n_move}: {e}")
#                 return [] # Return empty list if an invalid move is found
        
#         # Apply Black's move and get FEN (only if black_move exists and white's move was successful)
#         if black_move_san: # Only attempt if black_move exists
#             try:
#                 move_obj_black = board.parse_san(black_move_san)
#                 board.push(move_obj_black)
#                 fens_sequence.append(board.fen())
#             except (ValueError, chess.InvalidMoveError) as e:
#                 # print(f"Error applying Black's move '{black_move_san}' at move number {n_move}: {e}")
#                 return [] # Return empty list if an invalid move is found
                
#     return fens_sequence

# # --- Database Fetching Function ---
# async def get_all_moves_for_links_batch(game_links: List[int]) -> Dict[int, list[dict]]: # Changed type hint for game_links, and return dict key
#     """
#     Fetches all moves for a given list of game links in a single batched query
#     to the database asynchronously.
#     """
#     if not game_links:
#         return {}

#     sql_query = """
#     SELECT link, n_move, white_move, black_move
#     FROM moves
#     WHERE link = ANY(CAST(:game_links AS bigint[]))
#     ORDER BY link, n_move;
#     """
#     result_tuples = await open_async_request(sql_query, params={"game_links": game_links})

#     grouped_moves = defaultdict(list)
#     for row in result_tuples:
#         link, n_move, white_move, black_move = row
#         grouped_moves[link].append({
#             'n_move': n_move,
#             'white_move': white_move,
#             'black_move': black_move
#         })
#     return grouped_moves

# # --- Main FEN Generation ---
# async def get_fens_from_games_optimized(new_game_links_data: list[tuple]) -> list[str]:
#     """
#     Retrieves and generates unique FENs for a list of game links asynchronously.
#     """
#     all_fens = set()
#     game_links_only = [x[0] for x in new_game_links_data]

#     start_db_fetch = time.time()
#     # Fetch all moves for all games in a single batched query - NOW AWAITING
#     all_game_moves_grouped = await get_all_moves_for_links_batch(game_links_only)
#     db_fetch_time = time.time() - start_db_fetch
#     # print(f"Time to fetch all game moves from DB (batched): {db_fetch_time:.4f} seconds")

#     total_fen_generation_time = 0
#     games_processed = 0
    
#     for game_link in game_links_only:
#         game_moves = all_game_moves_grouped.get(game_link)
        
#         if game_moves:
#             fen_gen_start = time.time()
#             try:
#                 game_fens = generate_fens_for_single_game_moves(game_moves)
#                 all_fens.update(game_fens) # Add FENs to the set
#                 games_processed += 1
#             except Exception as e: # Catch any unexpected errors during FEN generation
#                 print(f"An unexpected error occurred while processing game {game_link}: {e}")
#                 # Continue to the next game even if one game fails
#             total_fen_generation_time += (time.time() - fen_gen_start)
#         else:
#             # print(f"No moves found in the database for game link: {game_link}")
#             pass # Suppress print for now

#     if games_processed > 0:
#         # print(f"Mean FEN generation time per game (excluding DB fetch): {total_fen_generation_time / games_processed:.4f} seconds")
#         pass # Suppress print for now
        
#     return list(all_fens) # Convert the set back to a list for the final output

# async def get_new_fens(posible_fens: list[str]) -> list[dict]: # Corrected return type hint
#     """
#     Compares a list of possible FENs against known FENs in the 'main_fen' table
#     and returns only the FENs that are not already present at the DB.

#     Args:
#         posible_fens (list[str]): A list of FEN strings to check.

#     Returns:
#         list[dict]: A list of dictionaries, where each dictionary contains
#                     'fen', 'n_games', and 'moves_counter' for new FENs.
#     """
#     if not posible_fens:
#         return []

#     sql_query = """
#     SELECT p_fen.f
#     FROM UNNEST(CAST(:posible_fens AS text[])) AS p_fen(f)
#     LEFT JOIN main_fen AS rf ON p_fen.f = rf.fen
#     WHERE rf.fen IS NULL;
#     """
#     # Use open_async_request and await it
#     result_tuples = await open_async_request(sql_query, params={"posible_fens": posible_fens})
#     new_raw_fens = list(chain.from_iterable(result_tuples))

#     new_fens_data = [simplify_fen_and_extract_counters_for_insert(f) for f in new_raw_fens]
    
#     return new_fens_data

# # --- Functions for Inserting Data ---
# async def insert_fens(fens_raw_from_games: List[Dict[str, Any]]):
#     """
#     Aggregates incoming FENs (handling duplicates within the batch),
#     then separates them into existing and new for bulk updates and inserts.
#     """
#     if not fens_raw_from_games:
#         print("No FENs to insert or update.")
#         return

#     # ---  Aggregate duplicate FENs within the incoming batch ---
#     aggregated_fens: Dict[str, Dict[str, Any]] = {}
#     for fen_data in fens_raw_from_games:
#         fen_str = fen_data['fen']
#         moves_counter = fen_data['moves_counter']

#         if fen_str not in aggregated_fens:
#             aggregated_fens[fen_str] = {
#                 'fen': fen_str,
#                 'n_games_in_batch': 1,
#                 'moves_counter_in_batch': moves_counter
#             }
#         else:
#             aggregated_fens[fen_str]['n_games_in_batch'] += 1
            
#             current_batch_moves = aggregated_fens[fen_str]['moves_counter_in_batch'].split('#') if aggregated_fens[fen_str]['moves_counter_in_batch'] else []
#             new_moves_from_current_occurrence = moves_counter.split('#') if moves_counter else []
            
#             for move_part in new_moves_from_current_occurrence:
#                 if move_part and move_part not in current_batch_moves:
#                     current_batch_moves.append(move_part)
            
#             aggregated_fens[fen_str]['moves_counter_in_batch'] = '#'.join(current_batch_moves)

#     fens_to_process = list(aggregated_fens.values())
#     print(f"Aggregated {len(fens_raw_from_games)} raw FENs down to {len(fens_to_process)} unique FENs for processing.")

#     incoming_fen_strings = [d['fen'] for d in fens_to_process]
    
#     async with AsyncDBSession() as session:
#         try:
#             existing_fens_map = {}

#             for i in range(0, len(incoming_fen_strings), EXISTING_FENS_QUERY_BATCH_SIZE):
#                 batch_fen_strings = incoming_fen_strings[i : i + EXISTING_FENS_QUERY_BATCH_SIZE]
                
#                 batch_existing_records = (await session.execute(
#                     select(MainFen.fen, MainFen.n_games, MainFen.moves_counter)
#                     .filter(MainFen.fen.in_(batch_fen_strings))
#                 )).all()

#                 for rec in batch_existing_records:
#                     existing_fens_map[rec.fen] = {
#                         'n_games': rec.n_games,
#                         'moves_counter': rec.moves_counter
#                     }
            
#             print(f"Identified {len(existing_fens_map)} FENs already in the database after all batches.")
#             if existing_fens_map:
#                 pass

#             fens_for_insert_batch = []
#             fens_for_update_batch = []

#             for processed_fen_data in fens_to_process:
#                 fen_str = processed_fen_data['fen']
#                 batch_n_games = processed_fen_data['n_games_in_batch']
#                 batch_moves_counter = processed_fen_data['moves_counter_in_batch']

#                 if fen_str in existing_fens_map:
#                     existing_db_data = existing_fens_map[fen_str]
                    
#                     updated_n_games = existing_db_data['n_games'] + batch_n_games

#                     current_db_moves_list = existing_db_data['moves_counter'].split('#') if existing_db_data['moves_counter'] else []
#                     current_batch_moves_list = batch_moves_counter.split('#') if batch_moves_counter else []
                    
#                     for move_part in current_batch_moves_list:
#                         if move_part and move_part not in current_db_moves_list:
#                             current_db_moves_list.append(move_part)
                    
#                     updated_moves_counter_str = '#'.join(current_db_moves_list)

#                     fens_for_update_batch.append({
#                         'fen': fen_str,
#                         'n_games': updated_n_games,
#                         'moves_counter': updated_moves_counter_str
#                     })
#                 else:
#                     fens_for_insert_batch.append(
#                         MainFenCreateData(
#                             fen=fen_str,
#                             n_games=batch_n_games,
#                             moves_counter=batch_moves_counter
#                         ).model_dump()
#                     )
            
#             if fens_for_insert_batch:
#                 try:
#                     await session.run_sync(
#                         lambda sync_session: sync_session.bulk_insert_mappings(MainFen, fens_for_insert_batch)
#                     )
#                     print(f"Successfully bulk inserted {len(fens_for_insert_batch)} new FENs.")
#                 except Exception as e:
#                     print(f"Error during bulk insert of new FENs: {e}")
#                     raise

#             if fens_for_update_batch:
#                 try:
#                     await session.run_sync(
#                         lambda sync_session: sync_session.bulk_update_mappings(MainFen, fens_for_update_batch)
#                     )
#                     print(f"Successfully bulk updated {len(fens_for_update_batch)} existing FENs.")
#                 except Exception as e:
#                     print(f"Error during bulk update of existing FENs: {e}")
#                     raise
            
#             await session.commit()
#             print("FEN insertion/update process complete.")

#         except Exception as e:
#             await session.rollback()
#             print(f"An unexpected error occurred during FEN processing: {e}")
#             raise

# async def insert_processed_game_links(links: list[tuple]):
#     """
#     Inserts a list of game links into the processed_game table asynchronously.
#     """
#     if not links:
#         print("No game links to insert into processed_game table.")
#         return

#     try:
#         to_insert_data = [{'link': x[0]} for x in links]
#         processed_links_data = [ProcessedGameCreateData(**data).model_dump() for data in to_insert_data]

#         processed_link_interface = DBInterface(ProcessedGame)
#         await processed_link_interface.create_all(processed_links_data)
#         print(f"Successfully inserted {len(links)} game links into processed_game.")
#     except Exception as e:
#         print(f"Error inserting game links into processed_game: {e}")
#         raise

# async def collect_fens_operations(n_games):
#     """
#     Orchestrates the collection of FENs from new games, their analysis,
#     and insertion into the database asynchronously.
#     """
#     new_game_links = await get_new_games_links(n_games)
#     start_total_fen_gen = time.time()
#     fen_set_from_games = await get_fens_from_games_optimized(new_game_links)
#     start_new_fens_check = time.time()
#     new_fens = await get_new_fens(fen_set_from_games)
#     print("\n--- Inserting data into the database ---")
#     start_insert_fens = time.time()
#     await insert_fens(new_fens)
#     print('insert_fens time elapsed: ', time.time() - start_insert_fens)
#     start_insert_games = time.time()
#     await insert_processed_game_links(new_game_links)
#     print('insert_games time elapsed: ', time.time() - start_insert_games)
#     return f"{len(new_fens)} NEW FENS from {len(new_game_links)} NEW GAMES"

# def simplify_fen_and_extract_counters_for_insert(full_fen: str) -> dict: # Changed return type hint
#     """
#     Takes a full FEN string, extracts the halfmove clock and fullmove number,
#     and returns the simplified FEN along with the extracted counters as a string.

#     Args:
#         full_fen (str): The complete FEN string (e.g., 'rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1').

#     Returns:
#         dict: A dictionary containing:
#             - "fen": The simplified FEN string.
#             - "n_games": Defaulted to 1 for initial insertion.
#             - "moves_counter": A string containing the halfmove clock and fullmove number, separated by '#'.
#     """
#     parts = full_fen.split(' ')

#     simplified_fen = full_fen # Default in case of malformed FEN
#     extracted_counters = ""

#     if len(parts) >= 4:
#         simplified_fen_parts = parts[0:4]
#         simplified_fen = ' '.join(simplified_fen_parts)

#         if len(parts) >= 6:
#             halfmove_clock = parts[4]
#             fullmove_number = parts[5]
#             extracted_counters = f"{halfmove_clock}#{fullmove_number}" # Remove leading/trailing '#' to be cleaner
#         else:
#             # print(f"Warning: FEN '{full_fen}' does not contain expected halfmove or fullmove numbers.")
#             pass # Suppress print
#     else:
#         # print(f"Error: Malformed FEN string provided: '{full_fen}'. Expected at least 4 parts.")
#         pass # Suppress print

#     return {"fen":simplified_fen, "n_games": 1, "moves_counter":extracted_counters}

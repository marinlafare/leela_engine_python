#OPERATIONS_COLLECT_FENS
import time
import chess
from collections import defaultdict
import asyncio
from itertools import chain
from fastapi import WebSocketDisconnect
from database.operations.connection import Connection
from database.database.ask_db import get_new_games_links, get_fens_from_games, get_new_fens, get_one_game,open_request
from database.operations.models import KnownfensCreateData, RawfenCreateData
from database.database.db_interface import DBInterface
from database.database.models import Knownfens, Rawfen


# --- Helper function: Generates FENs for a single game's moves ---
def _generate_fens_for_single_game_moves(moves: list[dict]) -> list[str]:
    """
    Generates a sequence of FENs for a single chess game given its moves.

    Args:
        moves (list[dict]): A list of dictionaries, where each dictionary represents
                            a move with keys 'n_move', 'white_move', 'black_move'.
                            Example: [{'n_move': 1, 'white_move': 'e4', 'black_move': 'e5'}]

    Returns:
        list[str]: A list of FEN strings representing the board state after each half-move.
                   Returns a partial list or an empty list if an invalid move is encountered,
                   along with a printed error message.
    """
    board = chess.Board() # Initialize a standard chess board
    fens_sequence = []
    
    for ind, move in enumerate(moves):
        # Basic assertion to check move order consistency, can be removed if not critical
        if move['n_move'] != ind + 1:
            print(f"Warning: n_move mismatch for move {move['n_move']} at index {ind}. Expected {ind + 1}. "
                  f"Processing might be out of order for this game.")
            # Decide if you want to continue or stop here based on data integrity needs.
            # For now, we continue but warn.
            
        n_move = move['n_move']
        white_move_san = move.get('white_move') # Use .get() for safer access
        black_move_san = move.get('black_move') # Use .get() for safer access

        # Apply White's move and get FEN
        if white_move_san: # Only attempt if white_move exists
            try:
                move_obj_white = board.parse_san(white_move_san)
                board.push(move_obj_white)
                fens_sequence.append(board.fen())
            except (ValueError, chess.InvalidMoveError) as e:
                print(f"Error applying White's move '{white_move_san}' at move number {n_move}: {e}")
                # Stop processing this game if an invalid move is found
                return fens_sequence
        
        # Apply Black's move and get FEN (only if black_move exists and white's move was successful)
        if black_move_san: # Only attempt if black_move exists
            try:
                move_obj_black = board.parse_san(black_move_san)
                board.push(move_obj_black)
                fens_sequence.append(board.fen())
            except (ValueError, chess.InvalidMoveError) as e:
                print(f"Error applying Black's move '{black_move_san}' at move number {n_move}: {e}")
                # Stop processing this game if an invalid move is found
                return fens_sequence
                
    return fens_sequence

# --- Optimized Database Fetching Function ---
def get_all_moves_for_links_batch(game_links: list[str]) -> dict[str, list[dict]]:
    """
    Fetches all moves for a given list of game links in a single batched query
    to the database.

    Args:
        game_links (list[str]): A list of game links (URLs or unique identifiers).

    Returns:
        dict[str, list[dict]]: A dictionary where keys are game links and values are lists of
                               move dictionaries, sorted by 'n_move' for each game.
                               Returns an empty dictionary if no links are provided or no
                               moves are found.
    """
    if not game_links:
        return {}

    # SQL query to select moves for all provided links.
    # Using UNNEST for the array parameter is efficient for PostgreSQL.
    # IMPORTANT FIX: Added ::bigint cast to ensure type compatibility with 'link' column.
    sql_query = """
    SELECT link, n_move, white_move, black_move
    FROM moves
    WHERE link IN (SELECT unnest(%s::text[])::bigint)
    ORDER BY link, n_move;
    """
    
    # open_request is assumed to execute the query with parameterized input
    # and return a list of tuples (link, n_move, white_move, black_move).
    result_tuples = open_request(sql_query, params=(game_links,))

    # Group the fetched moves by game link for easier processing
    grouped_moves = defaultdict(list)
    for row in result_tuples:
        link, n_move, white_move, black_move = row
        grouped_moves[link].append({
            'n_move': n_move,
            'white_move': white_move,
            'black_move': black_move
        })
    return grouped_moves

# --- Main FEN Generation Orchestrator (Optimized) ---
def get_fens_from_games_optimized(new_game_links_data: list[tuple]) -> list[str]:
    """
    Retrieves and generates unique FENs for a list of game links.
    This version is optimized to fetch all game moves in a single batched query
    to significantly reduce execution time.

    Args:
        new_game_links_data (list[tuple]): A list of tuples, where each tuple's
                                           first element is a game link.
                                           Typically, this comes from get_new_games_links.

    Returns:
        list[str]: A unique list of FEN strings generated from all processed games.
    """
    all_fens = set() # Use a set for efficient storage of unique FENs
    game_links_only = [x[0] for x in new_game_links_data] # Extract links from the input tuples

    start_db_fetch = time.time()
    # Fetch all moves for all games in a single batched query
    all_game_moves_grouped = get_all_moves_for_links_batch(game_links_only)
    db_fetch_time = time.time() - start_db_fetch
    print(f"Time to fetch all game moves from DB (batched): {db_fetch_time:.4f} seconds")

    total_fen_generation_time = 0
    games_processed = 0
    # Process each game's moves to generate FENs
    for game_link in game_links_only: # Iterate through the original list to ensure all links are attempted
        game_moves = all_game_moves_grouped.get(game_link)
        
        if game_moves:
            fen_gen_start = time.time()
            try:
                # Use the renamed helper function for single game FEN generation
                game_fens = _generate_fens_for_single_game_moves(game_moves)
                all_fens.update(game_fens) # Add FENs to the set
                games_processed += 1
            except Exception as e: # Catch any unexpected errors during FEN generation
                print(f"An unexpected error occurred while processing game {game_link}: {e}")
                # Continue to the next game even if one game fails
            total_fen_generation_time += (time.time() - fen_gen_start)
        else:
            print(f"No moves found in the database for game link: {game_link}")

    if games_processed > 0:
        print(f"Mean FEN generation time per game (excluding DB fetch): {total_fen_generation_time / games_processed:.4f} seconds")
    
    return list(all_fens) # Convert the set back to a list for the final output

def get_new_fens(posible_fens: list[str]) -> list[str]:
    """
    Compares a list of possible FENs against known FENs in the 'rawfen' table
    and returns only the FENs that are not already present.

    Args:
        posible_fens (list[str]): A list of FEN strings to check.

    Returns:
        list[str]: A list of FEN strings that are new (not in 'rawfen').
    """
    if not posible_fens:
        return []

    # REVISED SQL QUERY using LEFT JOIN and WHERE IS NULL for better performance
    sql_query = """
    SELECT p_fen.f
    FROM UNNEST(%s::text[]) AS p_fen(f)
    LEFT JOIN rawfen AS rf ON p_fen.f = rf.fen
    WHERE rf.fen IS NULL;
    """
    
    # open_request is assumed to handle the query and return results.
    result_tuples = open_request(sql_query, params=(posible_fens,))
    valid_fens = list(chain.from_iterable(result_tuples))
    return valid_fens

# --- Functions for Inserting Data (Adjusted for your DBInterface) ---
def insert_fens(fens: list[str]):
    """
    Inserts a list of new FENs into the rawfen table.
    """
    try:
        to_insert_fens = [RawfenCreateData(**{'fen':x}).model_dump() for x in fens]
        rawfen_interface = DBInterface(Rawfen) 
        rawfen_interface.create_all(to_insert_fens)
        print(f"Successfully inserted {len(fens)} FENs.")
    except Exception as e:
        print(f"Error inserting FENs: {e}")
        # The DBInterface.create_all method should handle internal rollback/commit/close
        # We re-raise if you want the outer script to know about the failure.
        raise

def insert_games(links: list[tuple]):
    """
    Inserts a list of game links into the knownfens table.
    """
    try:
        to_insert_games = [KnownfensCreateData(**{'link':x[0]}).model_dump() for x in links]
        game_interface = DBInterface(Knownfens) # Removed 'session=session'
        game_interface.create_all(to_insert_games)
        print(f"Successfully inserted {len(links)} game links.")
    except Exception as e:
        print(f"Error inserting game links: {e}")
        # The DBInterface.create_all method should handle internal rollback/commit/close
        # We re-raise if you want the outer script to know about the failure.
        raise
async def collect_fens_operations(n_games):
    
    new_game_links = get_new_games_links(n_games) 
    start_total_fen_gen = time.time()
    fen_set_from_games = get_fens_from_games_optimized(new_game_links)
    print(f'{len(fen_set_from_games)} fens from {len(new_game_links)}',time.time()-start_total_fen_gen)
    start_new_fens_check = time.time()
    new_fens = get_new_fens(fen_set_from_games)
    print(f'{len(new_fens)} fens','time elapsed: ',time.time()-start_new_fens_check)
    
    print("\n--- Inserting data into the database ---")
    start_insert_fens = time.time()
    insert_fens(new_fens)
    print('insert_fens time elapsed: ', time.time() - start_insert_fens)
    
    start_insert_games = time.time()
    insert_games(new_game_links)
    print('insert_games time elapsed: ', time.time() - start_insert_games)
    return f"{len(new_fens)} NEW FENS from {len(new_game_links)} NEW GAMES"


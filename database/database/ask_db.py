#DATABASE_ASK_DB
import os
import requests
import pandas as pd
import tempfile
import psycopg2
from itertools import chain
from constants import CONN_STRING, PORT

def get_ask_connection():    
    return psycopg2.connect(CONN_STRING, port = PORT)
def get_one_game(link):
    conn = get_ask_connection()
    try:
        with conn.cursor() as curs:
            curs.execute(f"select moves.n_move, white_move, black_move from moves where moves.link = '{link}'")
            column_names = [desc[0] for desc in curs.description]
            results = []
            for row in curs.fetchall():
                results.append(dict(zip(column_names, row)))
            return results
    finally:
        conn.close()
def open_request_1(sql_question:str):
    conn = get_ask_connection()
    with conn.cursor() as curs:
        curs.execute(
            sql_question
        )
        result = curs.fetchall()
    return result
def open_request(sql_question: str, params: tuple = None, fetch_as_dict: bool = False):
    conn = get_ask_connection()
    try:
        with conn.cursor() as curs:
            if params:
                curs.execute(sql_question, params)
            else:
                curs.execute(sql_question)
            
            if fetch_as_dict:
                column_names = [desc[0] for desc in curs.description]
                results = []
                for row in curs.fetchall():
                    results.append(dict(zip(column_names, row)))
                return results
            else:
                return curs.fetchall()
    finally:
        conn.close()
def get_all_tables():
    conn = get_ask_connection()
    conn.autocommit = True
    with conn.cursor() as curs:
        curs.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE';
            """
        )
        tables = curs.fetchall()
    return tables
def delete_all_leela_tables():
    for table_row in ['fen','known_fens','rawfen','knownfens','fens_ready']:
            conn = get_ask_connection()
            conn.autocommit = True
            table_name = table_row
            print(f"Deleting table: {table_name}...")
            try:
                with conn.cursor() as curs:
                    curs.execute(f"DROP TABLE IF EXISTS \"{table_name}\" CASCADE;")
                    print(f"Successfully deleted table: {table_name}")
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
            finally:
                if conn:
                    conn.close()
                    print("Database connection closed.")
def get_new_games_links(n_games):
    valid_links = open_request(f""" SELECT link
                                    FROM game
                                    WHERE link NOT IN (SELECT link FROM knownfens)
                                    LIMIT {n_games};
                                    """)
    return valid_links
    




def get_fens_from_games(moves):
    board = chess.Board() # Initialize a standard chess board
    fens_sequence = []
    for ind, move in enumerate(moves):
        assert move['n_move'] == ind+1
        n_move = move['n_move']
        white_move_san = move['white_move']
        black_move_san = move['black_move']
        current_fens = {'n_move': n_move}
        
        try:
            move_obj_white = board.parse_san(white_move_san)
            board.push(move_obj_white)
            fens_sequence.append(board.fen())
        except ValueError as e:
            print(f"Error applying White's move '{white_move_san}' at move number {n_move}: {e}")
            return fens_sequence # Stop processing on error
        except chess.InvalidMoveError as e:
            print(f"Invalid White's move '{white_move_san}' at move number {n_move}: {e}")
            return fens_sequence # Stop processing on error


        # 2. Apply Black's move and get FEN
        try:
            move_obj_black = board.parse_san(black_move_san)
            board.push(move_obj_black)
            fens_sequence.append(board.fen())
        except ValueError as e:
            print(f"Error applying Black's move '{black_move_san}' at move number {n_move}: {e}")
            return fens_sequence # Stop processing on error
        except chess.InvalidMoveError as e:
            print(f"Invalid Black's move '{black_move_san}' at move number {n_move}: {e}")
            return fens_sequence # Stop processing on error

        #fens_sequence.append(current_fens)
    return fens_sequence


def get_new_fens(posible_fens: list[str]) -> list[str]:
    """
    Filters a list of FENs, returning only those not already in the database.
    This is highly efficient as the filtering is done by the database.

    Args:
        posible_fens (list[str]): A list of FEN strings to check for existence.

    Returns:
        list[str]: A list of FEN strings from `posible_fens` that are not found
                   in the 'fen' table. Returns an empty list if `posible_fens` is empty.
    """
    if not posible_fens:
        return [] # No FENs to check, return an empty list

    sql_query = """
    SELECT p_fen.f
    FROM UNNEST(%s::text[]) AS p_fen(f)  -- 'p_fen' is an alias for the unnested array, 'f' is the column name
    WHERE p_fen.f NOT IN (SELECT fen FROM rawfen); -- FIX IS HERE: Changed 'known_fens' to 'fen' and 'link' to 'fen'
    """
    
    result_tuples = open_request(sql_query, params=(posible_fens,))
    
    valid_fens = list(chain.from_iterable(result_tuples))
    
    return valid_fens
# def get_new_fens(posible_fens):
#     in_db_fens = open_request(f"""
#                                 SELECT fen.fen from fen;
#                                 """)
#     in_db_fens = set(in_db_fens)
#     valid_fens = list(posible_fens - in_db_fens)
#     return valid_fens
















    
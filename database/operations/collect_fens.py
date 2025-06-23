#OPERATIONS
import chess
from fastapi import WebSocketDisconnect
from database.operations.connection import Connection
from database.database.ask_db import get_new_games_links, get_fens_from_games, get_new_fens, get_one_game
from database.operations.models import KnownfensCreateData, RawfenCreateData
from database.database.db_interface import DBInterface
from database.database.models import Knownfens, Rawfen

def get_fens_from_moves(moves):
    board = chess.Board() # Initialize a standard chess board
    fens_sequence = []
    for ind, move in enumerate(moves):
        assert move['n_move'] == ind+1
        n_move = move['n_move']
        white_move_san = move['white_move']
        black_move_san = move['black_move']
        
        
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
def get_fens_from_games(new_game_links):
    fens = []
    new_game_links = [x[0] for x in new_game_links]
    for game_link in new_game_links:
        game_moves = get_one_game(game_link)
        try:
            fens.extend(get_fens_from_moves(game_moves))
        except:
            continue
    return list(set(fens))
def insert_fens(fens):
    to_insert_fens = [RawfenCreateData(**{'fen':x}).model_dump() for x in fens]
    rawfen_interface = DBInterface(Rawfen)
    rawfen_interface.create_all(to_insert_fens)
def insert_games(links):
    to_insert_fens = [KnownfensCreateData(**{'link':x[0]}).model_dump() for x in links]
    game_interface = DBInterface(Knownfens)
    game_interface.create_all(to_insert_fens)

async def collect_fens(websocket, n_games):
    connection = Connection()
    await connection.connect(websocket)
    try:
        new_game_links = get_new_games_links(n_games)
        if len(new_game_links) == 0:
            await connection.send_personal_json({"fuck_you_dormamu":"i came to bargain",
                                                    "new_games":"NO NEW GAMES BYEEEE"}, websocket)
            return 
        await connection.send_personal_json({"fuck_you_dormamu":"i came to bargain",
                                                    "new_games":len(new_game_links)}, websocket)
        
        fen_set_from_games = get_fens_from_games(new_game_links)
        await connection.send_personal_json({"fuck_you_dormamu":"i came to bargain",
                                                    "FENs: ":len(fen_set_from_games)}, websocket)
        new_fens = get_new_fens(fen_set_from_games)
        if len(new_fens) == 0:
            await connection.send_personal_json({"fuck_you_dormamu":"i came to bargain",
                                                    "new_FENS":"NO NEW FENS BYEEEE"}, websocket)
            insert_games(new_game_links)
            return
        await connection.send_personal_json({"fuck_you_dormamu":"i came to bargain",
                                                    "new_FENs":len(new_fens)}, websocket)
        
        insert_fens(new_fens)
        await connection.send_personal_json({"fuck_you_dormamu":"i came to bargain",
                                                    "FENS INSERTED: ":len(new_fens)}, websocket)
        insert_games(new_game_links)
        await connection.send_personal_json({"fuck_you_dormamu":"i came to bargain",
                                                    "GAME LINKS INSERTED: ":len(new_game_links)}, websocket)
        
    except WebSocketDisconnect:
        print(f'CLIENT  Dissconenected')

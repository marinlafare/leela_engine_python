import chess
from typing import List, Dict, Union

def verify_game_moves_against_fens(
    game_moves: List[Dict[str, Union[int, str]]],
    game_fens: List[Dict[str, str]]
) -> bool:
    """
    Verifies if a list of FENs corresponds to a sequence of chess moves,
    matching only the first four fields of the FEN.

    Args:
        game_moves: A list of dictionaries, where each dictionary represents a move
                    with 'n_move', 'white_move', and 'black_move'.
        game_fens: A list of dictionaries, where each dictionary contains a 'fen' string.
                   This list might not be sorted by move number and contains
                   simplified FENs (first 4 fields).

    Returns:
        True if all moves generate a simplified FEN that is present in the provided
        simplified FENs list, False otherwise.
    """
    board = chess.Board()  # Initialize the board to the starting position
    
    # Create a set of simplified FENs from the game_fens list for efficient lookup
    # We'll store (simplified FEN string, is_initial_board) tuples.
    provided_simplified_fens = {(' '.join(fen_entry['fen'].split(' ')[:4]), False) for fen_entry in game_fens}

    # Get the simplified FEN of the initial board
    initial_simplified_fen = ' '.join(board.fen().split(' ')[:4])
    
    # Add the initial board's simplified FEN to the set of FENs we expect
    provided_simplified_fens.add((initial_simplified_fen, True)) # True indicates it's the initial board FEN

    # Sort the game_moves list by 'n_move' to ensure correct game progression
    sorted_moves = sorted(game_moves, key=lambda x: x['n_move'])

    #print(f"Initial Board Simplified FEN: {initial_simplified_fen}")

    # Track which simplified FENs from provided_simplified_fens have been successfully matched
    matched_simplified_fens = set()
    
    # Check if the initial board's simplified FEN is in the provided_simplified_fens
    if (initial_simplified_fen, True) not in provided_simplified_fens:
        #print(f"Error: Initial board simplified FEN '{initial_simplified_fen}' not found in provided FENs.")
        return False
    matched_simplified_fens.add((initial_simplified_fen, True))

    for move_data in sorted_moves:
        n_move = move_data['n_move']
        white_move_san = move_data.get('white_move')
        black_move_san = move_data.get('black_move')

        # Apply White's move
        if white_move_san:
            try:
                move = board.parse_san(white_move_san)
                board.push(move)
                current_simplified_fen = ' '.join(board.fen().split(' ')[:4])
                #print(f"After White's move {n_move} ({white_move_san}): {current_simplified_fen}")
                if (current_simplified_fen, False) not in provided_simplified_fens and \
                   (current_simplified_fen, True) not in provided_simplified_fens:
                    #print(f"Mismatch: Simplified FEN '{current_simplified_fen}' after White's move {n_move} ({white_move_san}) not found in provided FENs.")
                    return False
                matched_simplified_fens.add((current_simplified_fen, False)) # Mark as matched
            except ValueError as e:
                #print(f"Error parsing White's move {n_move} '{white_move_san}': {e}")
                return False
            except Exception as e:
                #print(f"An unexpected error occurred with White's move {n_move} '{white_move_san}': {e}")
                return False

        # Apply Black's move
        if black_move_san:
            try:
                move = board.parse_san(black_move_san)
                board.push(move)
                current_simplified_fen = ' '.join(board.fen().split(' ')[:4])
                #print(f"After Black's move {n_move} ({black_move_san}): {current_simplified_fen}")
                if (current_simplified_fen, False) not in provided_simplified_fens and \
                   (current_simplified_fen, True) not in provided_simplified_fens:
                    #print(f"Mismatch: Simplified FEN '{current_simplified_fen}' after Black's move {n_move} ({black_move_san}) not found in provided FENs.")
                    return False
                matched_simplified_fens.add((current_simplified_fen, False)) # Mark as matched
            except ValueError as e:
                #print(f"Error parsing Black's move {n_move} '{black_move_san}': {e}")
                return False
            except Exception as e:
                #print(f"An unexpected error occurred with Black's move {n_move} '{black_move_san}': {e}")
                return False

    #print("All generated simplified FENs correspond to simplified FENs in the provided list.")
    return True
async def make_link_moves_dict(link):
    moves = await get_fens_by_game_link(link)
    return {link:moves}
async def get_test_data_for_player(username: str, limit: int = 3) -> List[int]:
    sql_query = """
    SELECT g_inner.link
    FROM (
        SELECT g.link
        FROM game AS g
        LEFT JOIN game_fen_association AS gfa ON g.link = gfa.game_link
        WHERE (g.white = :username OR g.black = :username)
        ORDER BY RANDOM()
        LIMIT :limit
    ) AS g_inner
    GROUP BY g_inner.link;
    """
    # Use open_async_request and await it. Fetch values directly as links.
    game_links = await open_async_request(sql_query, params={"username": username, "limit": limit})

    game_links = [x[0] for x in game_links]
    game_moves = [await get_one_game_with_link(x) for x in game_links]
    game_moves_dict = {x[0]['link']:[] for x in game_moves}

    for game in game_moves:
        for move in game:
            move = dict(move)
            link = move.pop('link')
            game_moves_dict[link].append(move)
            
    game_fens = [await make_link_moves_dict(x) for x in game_links]
    game_fens = {list(x.keys())[0]:x[list(x.keys())[0]] for x in game_fens}
    games = await get_games_data_from_links(list_of_links = game_links)

    return game_links, games, game_moves_dict, game_fens#[link[0] for link in result_tuples]

async def check_variant(game_moves):
    if '@' in ''.join([x['white_move'] for x in game_moves]):
            variant = 'CrazyHouse'
    elif '@' in ''.join([x['black_move'] for x in game_moves]):
            variant = 'CrazyHouse'
    else:
        variant = 'maybe_classic'
    return variant
async def check_player_pipeline(player_name, limit, return_data:bool = False):
    game_links, games, game_moves, game_fens = await get_test_data_for_player(player_name, limit)
    test_report_positive = {x:{} for x in game_links}
    test_report_negative = {x:{} for x in game_links}
    for link in game_links:
        game = games[link]
        
        n_moves_validation = game['n_moves'] == max([x['n_move'] for x in game_moves[link]])
        moves_validation = verify_game_moves_against_fens(game_moves[link],game_fens[link])

        variant = await check_variant(game_moves[link])
        if variant == 'CrazyHouse' and n_moves_validation:
            test_report_positive[link]['moves_validation'] = moves_validation
            test_report_positive[link]['n_moves_validation'] = n_moves_validation
            test_report_positive[link]['variant'] = variant
        elif not moves_validation or not n_moves_validation:
            test_report_negative[link]['moves_validation'] = moves_validation
            test_report_negative[link]['n_moves_validation'] = n_moves_validation
            test_report_negative[link]['variant'] = variant
        else:
            test_report_positive[link]['moves_validation'] = moves_validation
            test_report_positive[link]['n_moves_validation'] = n_moves_validation
            test_report_positive[link]['variant'] = variant
    if all(isinstance(value, dict) and not value for value in test_report_negative.values()):
        test_report_negative = "NO NEGATIVES CONGRATULATIONS"
    if return_data:
        
        checking_results = {'game_links':game_links,
                           'games':games,
                           'game_moves':game_moves,
                           'game_fens':game_fens,
                           'test_report_positive':test_report_positive,
                            'test_report_negative':test_report_negative
                           }
    else:
        return {'test_report_negative':test_report_negative}
    return checking_results

async def test_player_game_move_fen(player_name, limit, return_data):
    checking_results = check_player_pipeline(player_name = player_name,
                                              limit = limit,
                                              return_data = return_data)
    return checking_results 
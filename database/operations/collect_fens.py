#OPERATIONS
from database.operations.connection import Connection

async def collect_fens(websocket, n_games):
    # new_game_links = get_new_games_links(n_games)
    # fen_set_from_games get_fen_set(new_game_links)


    connection = Connection()
    await connection.connect(websocket)

    try:
        
        while True:
            await connection.send_personal_json({"fuck_you_dormamu":"i came to bargain"}, websocket)
            user_response = await websocket.receive_text()
            if user_response == 'break':
                break
    except WebSocketDisconnect:
        print(f'CLIENT {count_id} Dissconenected')
    return 

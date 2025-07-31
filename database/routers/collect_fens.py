# database.routers.collect_fens.py

import asyncio
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from database.database.ask_db import get_players_with_names
from database.operations.fen import insert_fens_from_player

router = APIRouter()

@router.post("/collect_fens/all_players_fen")
async def api_fetch_new_fens():
    try:
        available_players = await get_players_with_names()
        for player in available_players:
            print(f'player {player}::::::::::')
            await insert_fens_from_player(player['player_name'], game_batches = 1000)
    # print(f'player: {available_players[1]['player_name']}')
    # await insert_fens_from_player(available_players[1]['player_name'], game_batches = 1000)

        return "FENS UPDATED SUCCESSFULY or something"
    except Exception as e:
        print(f"Error in API endpoint for Collecting FENS : {e}")
        return JSONResponse(content={"error": f"Failed to analyze FEN: {e}"}, status_code=500)

#ROUTERS

import asyncio
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from database.operations.collect_fens import collect_fens 

router = APIRouter()

@router.get("/collect_fens/{n_fens}")
async def api_read_player_fen_analysis(n_games: int):
    try:
        fen_analysis = collect_fens(n_games)
        return JSONResponse(content=fen_analysis)
    except Exception as e:
        print(f"Error in API endpoint for Collecting FENS : {e}")
        return JSONResponse(content={"error": f"Failed to analyze FEN: {e}"}, status_code=500)

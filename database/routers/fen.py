# database.routers.fen.py

import asyncio
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from database.operations.fen import analize_fen, get_weights_list
from database.operations.run_top_fen_analysis import analize_repeated_fens_operations
router = APIRouter()

@router.get("/fen/weights")
async def api_get_weights():
    from constants import LC0_DIRECTORY
    try:
        weights = await get_weights_list(LC0_DIRECTORY) 
        return JSONResponse(content=weights)
    except Exception as e:
        print(f"Error asking for the weigths: {e}")
        return JSONResponse(content={"error": f"Failed to analyze FEN: {e}"}, status_code=500)

@router.get("/fen/{fen:path}")
async def api_read_player_fen_analysis(fen: str, weights:str = 't1-256x10-distilled-swa-2432500.pb.gz'):
    """
    Analyzes a chess FEN position using the Leela engine.
    

    Args:
        fen: The FEN string of the chess position.

    Returns:
        JSONResponse: A JSON response containing the analysis results.
    """
    try:
        fen_analysis = await analize_fen(fen, weights) 
        return JSONResponse(content=fen_analysis)
    except Exception as e:
        print(f"Error in API endpoint for FEN {fen}: {e}")
        return JSONResponse(content={"error": f"Failed to analyze FEN: {e}"}, status_code=500)
@router.post("/fen/data")
async def api_analyse_repeated_fens(data:dict):
    """
    {
        "meta_repetitions":3,
        "n_repetitions":3,
        "between_min" : 4.0,
        "between_max": 600000.0,
        "limit": 50,
        "min_wait_time_seconds": 20,
        "max_wait_time": 0.2,
        "verbose_each":100,
        "analyse_time_limit":1.5,
        "nodes_limit": 100000,
        "weights": "t1-256x10-distilled-swa-2432500.pb.gz",
        "card": 0
    }
    """
    try:
        for turn in range(data['meta_repetitions']):
            print(f'############################')
            print(f'METAREPETITION: {turn}')
            print(f'############################')
            
            await analize_repeated_fens_operations(**data) 
        
        return JSONResponse(content=data)
    except Exception as e:
        print(f"Somthing happened while trying to start the analysis: {e}")
        return JSONResponse(content={"error": f"Failed to analyze FENs: {e}"}, status_code=500)

@router.post("/fen_from_player/{player_name}")
async def api_gather_fens_from_player(player_name: str):
    try:
        
        result = await gather_players_fens(player_name) 
        
        return JSONResponse(content=result)
    except Exception as e:
        print(f"Somthing happened while trying to start the analysis: {e}")
        return JSONResponse(content={"error": f"Failed to analyze FEN: {e}"}, status_code=500)


        
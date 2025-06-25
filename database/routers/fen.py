#ROUTERS_FEN

import asyncio
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from database.operations.fen import analize_fen 

router = APIRouter()

@router.get("/fen/{fen:path}")
async def api_read_player_fen_analysis(fen: str):
    """
    Analyzes a chess FEN position using the Lc0 (leela) engine.

    Args:
        fen: The FEN string of the chess position.

    Returns:
        JSONResponse: A JSON response containing the analysis results.
    """
    try:
        fen_analysis = await analize_fen(fen) 
        return JSONResponse(content=fen_analysis)
    except Exception as e:
        print(f"Error in API endpoint for FEN {fen}: {e}")
        return JSONResponse(content={"error": f"Failed to analyze FEN: {e}"}, status_code=500)

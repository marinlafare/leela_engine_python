# OPERATIONS
import chess
import chess.engine

from database.database.leela_engine import *

import os
import chess
import chess.engine
import asyncio

# --- Configuration (Ensure these paths are correct for your WSL environment) ---
LC0_PATH = ["/home/jon/workshop/leela_zero/leela_engine_python/Lc0/lc0.exe"]
lc0_directory = os.path.dirname(LC0_PATH[0])
LC0_WEIGHTS_FILE = "791556.pb.gz"
# --- End Configuration ---


async def initialize_lc0_engine() -> chess.engine.UciProtocol:
    """
    Launches and configures the Leela Chess Zero (Lc0) engine once.

    Returns:
        An initialized Lc0 engine instance (chess.engine.UciProtocol).

    Raises:
        Exception: If the engine fails to launch or configure.
    """
    engine_uci = None
    try:
        print("Launching Lc0 engine...")
        transport, engine_uci = await chess.engine.popen_uci(
            LC0_PATH, 
            cwd=lc0_directory
        )
        
        await engine_uci.configure({
            "WeightsFile": LC0_WEIGHTS_FILE,
            "Backend": "cuda-fp16",
            "Threads": 1,
            "MinibatchSize": 1024
        })
        print("Lc0 engine successfully launched and configured.")
        return engine_uci
    except Exception as e:
        print(f"Error initializing Lc0 engine: {e}")
        if engine_uci:
            await engine_uci.quit() # Ensure clean shutdown even on init failure
        raise # Re-raise the exception to propagate the error


async def analyze_single_position(engine_uci: chess.engine.UciProtocol, fen: str, nodes_limit: int = 50000) -> dict:
    """
    Analyzes a single chess position using an already initialized Leela Chess Zero (Lc0) engine
    and returns its centipawn score and principal variation.

    Args:
        engine_uci: An already initialized Lc0 engine instance.
        fen: The FEN string of the position to analyze.
        nodes_limit: The maximum number of nodes Lc0 should explore (playouts).

    Returns:
        A dictionary containing the FEN, centipawn score, and principal variation (PV).
        Includes an 'error' key if analysis fails.
    """
    board = chess.Board(fen)

    try:
        limit = chess.engine.Limit(nodes=nodes_limit)
        info = await engine_uci.analyse(board, limit=limit) # This line *must* be awaited
        
        score = info["score"].white().score(mate_score=10000) 
        pv = [move.uci() for move in info["pv"]]

        return {"fen": fen, "score": score, "pv": pv}

    except Exception as e:
        print(f"An error occurred during Lc0 analysis for FEN {fen}: {e}")
        return {"fen": fen, "error": str(e)}


# THIS IS THE PRIMARY FUNCTION THAT NEEDS TO BE CORRECTED IN YOUR OPERATIONS FILE
async def analize_fen(fen: str) -> dict:
    """
    Analyzes a single FEN position using the Lc0 engine.
    This function is asynchronous and manages the engine's lifecycle for a single request.

    Args:
        fen: The FEN string of the chess position.

    Returns:
        A dictionary containing the FEN, centipawn score, and principal variation (PV).
        Includes an 'error' key if analysis fails.
    """
    engine = None # Initialize engine to None
    try:
        # VERY IMPORTANT: AWAIT the engine initialization!
        engine = await initialize_lc0_engine() 
        
        # VERY IMPORTANT: AWAIT the single position analysis!
        analysis_result = await analyze_single_position(engine, fen, nodes_limit=50000)
        
        return analysis_result
    except Exception as e:
        print(f"Error in analize_fen for FEN {fen}: {e}")
        # Return an error dictionary, which is JSON serializable
        return {"fen": fen, "error": f"Analysis failed: {e}"}
    finally:
        # Always quit the engine when done or if an error occurs
        if engine:
            print(f"Quitting Lc0 engine for FEN {fen}...")
            await engine.quit()
            print(f"Lc0 engine quit for FEN {fen}.")









# def analize_one_fen(engine, fen, nodes_limit=50000):
#     result = {}
#     board = chess.Board(fen)
#     limit = chess.engine.Limit(nodes=nodes_limit)
#     info = engine.analyse(board, limit=limit)
#     result[fen] = info
#     result[fen]['score'] = info["score"].white().score(mate_score=10000) / 100
#     result[fen]['pv'] = [move.uci() for move in result[fen]['pv']]
#     return result


# async def analize_fen(fen: str) -> dict:
#     """
#     Analyzes a single FEN position using the Lc0 engine.
#     This function *must* be asynchronous and handle engine lifecycle.
#     """
#     engine = None # Initialize engine to None
#     try:
#         # Crucially, AWAIT the engine initialization!
#         engine = await initialize_lc0_engine() 
        
#         # Then, AWAIT the single position analysis using the obtained engine object
#         analysis_result = await analize_one_fen(engine, fen, nodes_limit=50000)
        
#         return analysis_result
#     except Exception as e:
#         print(f"Error in analize_fen for FEN {fen}: {e}")
#         return {"fen": fen, "error": f"Analysis failed: {e}"}
#     finally:
#         # Always quit the engine when done or if an error occurs
#         if engine:
#             await engine.quit()


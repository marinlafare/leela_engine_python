# OPERATIONS
import os
import chess
import chess.engine
import asyncio
from constants import LC0_PATH, lc0_directory, LC0_WEIGHTS_FILE
from database.operations.models import FenCreateData
from database.database.db_interface import DBInterface
from database.database.models import Fen, Game
async def initialize_lc0_engine() -> chess.engine.UciProtocol:
    """
    Launches and configures the Leela Chess Zero (Lc0) engine.
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

        weights = os.path.join(lc0_directory, LC0_WEIGHTS_FILE)
        await engine_uci.configure({
            "WeightsFile": weights,
            "Backend": "cuda-fp16",
            "Threads": 1,
            "MinibatchSize": 1024
        })

        return engine_uci
    except Exception as e:
        print(f"Error initializing Lc0 engine: {e}")
        if engine_uci:
            await engine_uci.quit()
        raise

async def analyze_single_position(engine_uci: chess.engine.UciProtocol,
                                  fen: str, nodes_limit: int = 50000) -> dict:
    """
    Analyzes a fen with Leela.
    
    Args:
        engine_uci: Leela engine instance.
        fen: a self explanatory fen.
        nodes_limit: The maximum number of nodes Lc0 should explore, default:50_000.

    Returns:
        A dictionary containing score in centipawns and principal variation and stuff.
    """
    board = chess.Board(fen)

    # "2 validation errors for FenCreateData\nfen\n  Field required [type=missing, input_value={'depth': 12, 'seldepth':...'d7d5', 'f3e5', 'd5e4']}, input_type=dict]\n    For further information visit https://errors.pydantic.dev/2.10/v/missing\nnode_per_second\n  Field required [type=missing, input_value={'depth': 12, 'seldepth':...'d7d5', 'f3e5', 'd5e4']}, input_type=dict]\n    For further information visit https://errors.pydantic.dev/2.10/v/missing"

    try:
        limit = chess.engine.Limit(nodes=nodes_limit)
        info = await engine_uci.analyse(board, limit=limit)
        score = info["score"].white().score(mate_score=10000) / 100
        pv = [move.uci() for move in info["pv"]]
        info['fen'] = fen
        info['score'] = score
        info['pv'] = ''.join([move+'##' for move in pv])
        print(info)
        fen_interface = DBInterface(Fen)
        fen_data = FenCreateData(**info)
        fen_interface.create(fen_data.model_dump())
        
        return info

    except Exception as e:
        print(f"An error occurred during Lc0 analysis for FEN {fen}: {e}")
        return {"fen": fen, "error": str(e)}


async def analize_fen(fen: str) -> dict:
    """
    Initializes a Leela engine instance.
    Asks the engine for the analysis of the fen.
    
    Args:
        fen: a string in a fen format.
    Returns:
        A dictionary containing the fen as key and the stuff as values.
    """
    engine = None
    try:
        engine = await initialize_lc0_engine() 
        analysis_result = await analyze_single_position(engine, fen, nodes_limit=50000)
        return analysis_result
    except Exception as e:
        print(f"Error in analize_fen for FEN {fen}: {e}")
        return {"fen": fen, "error": f"Analysis failed: {e}"}
    finally:
        if engine:
            print(f"Quitting Lc0 engine for FEN {fen}...")
            await engine.quit()
            print(f"Lc0 engine quit for FEN {fen}.")
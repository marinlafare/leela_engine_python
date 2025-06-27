#DATABASE_LEELA_ENGINE
import os
import chess
import chess.engine
import asyncio

from constants import LC0_PATH, lc0_directory, LC0_WEIGHTS_FILE

async def initialize_lc0_engine() -> chess.engine.UciProtocol:
    engine_uci = None
    try:
        print("...Starting Leela engine...")
        transport, engine_uci = await chess.engine.popen_uci(LC0_PATH, cwd=lc0_directory)
        await engine_uci.configure({
                                    "WeightsFile": LC0_WEIGHTS_FILE,
                                    "Backend": "cuda-fp16", 
                                    "Threads": 1,
                                    "MinibatchSize": 1024 
                                })
        print("...Leela engine ready...")
        return engine_uci
    except Exception as e:
        print(f"Error initializing Lc0 engine: {e}")
        if engine_uci:
            await engine_uci.quit()
        raise

print('leela_engine', LC0_PATH)
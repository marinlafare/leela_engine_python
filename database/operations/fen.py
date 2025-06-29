import os
import chess
import chess.engine
import asyncio
import time
import psutil
import json
import re
import logging
import subprocess # Ensure subprocess is imported for get_system_metrics

# Configure logging for chess.engine to suppress verbose tracebacks
logging.getLogger("chess.engine").setLevel(logging.ERROR)
logging.getLogger("asyncio").setLevel(logging.ERROR)

from constants import LC0_PATH, lc0_directory, LC0_WEIGHTS_FILE
from database.operations.models import FenCreateData
from database.database.db_interface import DBInterface
from database.database.models import Fen, MainFen, ProcessedGame
from database.database.ask_db import open_async_request, get_game_links_by_username
# from database.operations.collect_data import (generate_fens_for_single_game_moves,
#                                                 get_all_moves_for_links_batch,
#                                                 insert_processed_game_links, simplify_fen_and_extract_counters_for_insert)


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
        print("...Starting Leela engine...")
        # Removed stderr=subprocess.DEVNULL to show all stderr output
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

async def analyze_single_position(engine: chess.engine.UciProtocol, fen: str, n_nodes_limit: int = 50_000, time_limit: float = 3.0) -> FenCreateData:
    """
    Analyzes a single chess position (FEN) using the provided Leela engine.
    Returns analysis data or a default "failed" entry if analysis fails.
    """ 
    try:
        board = chess.Board(fen)
        info = await engine.analyse(board, chess.engine.Limit(time=time_limit,nodes=n_nodes_limit))

        # Extracting relevant information
        score_cp = info.get("score").white().score(mate_score=100000) # Score in centipawns from White's perspective
        depth = info.get("depth")
        seldepth = info.get("seldepth")
        time_taken = info.get("time")
        nodes = info.get("nodes")
        tbhits = info.get("tbhits", 0) # Default to 0 if not present
        nps = info.get("nps", 0) # Default to 0 if not present

        return FenCreateData(
            fen=fen,
            depth=depth,
            seldepth=seldepth,
            time=time_taken,
            nodes=nodes,
            score=score_cp,
            tbhits=tbhits,
            nps=nps
        )
    except chess.engine.EngineError as ee:
        print(f"Engine error analyzing FEN {fen}: {ee}. Problematic FEN: {fen}. Marking as failed analysis.")
        return FenCreateData(
            fen=fen,
            depth=0,      # Indicate failed analysis
            seldepth=0,   # Indicate failed analysis
            time=0.0,     # Indicate failed analysis
            nodes=0,      # Indicate failed analysis
            score=-999999.0, # A clear indicator of a failed or invalid score
            tbhits=0,
            nps=0
        )
    except asyncio.TimeoutError:
        print(f"Analysis timed out for FEN {fen} after {time_limit} seconds. Marking as failed analysis.")
        return FenCreateData(
            fen=fen,
            depth=0,
            seldepth=0,
            time=time_limit, # Or 0.0 if preferred
            nodes=0,
            score=-999999.0,
            tbhits=0,
            nps=0
        )
    except ValueError as ve:
        print(f"Value error (e.g., invalid FEN or engine output) for FEN {fen}: {ve}. Marking as failed analysis.")
        return FenCreateData(
            fen=fen,
            depth=0,
            seldepth=0,
            time=0.0,
            nodes=0,
            score=-999999.0,
            tbhits=0,
            nps=0
        )
    except Exception as e:
        print(f"An unexpected error occurred during analysis for FEN {fen}: {e}. Marking as failed analysis.")
        return FenCreateData(
            fen=fen,
            depth=0,
            seldepth=0,
            time=0.0,
            nodes=0,
            score=-999999.0,
            tbhits=0,
            nps=0
        )


async def analize_fen(fen: str,n_nodes_limit:int = 50_000):
    engine = None
    try:
        engine = await initialize_lc0_engine()
        analysis_data = await analyze_single_position(engine, fen,nodes_limit)
        # You would typically insert this data into the database here
        print(f"Analysis for {fen}: {analysis_data}")
        return analysis_data
    except Exception as e:
        print(f"Failed to analyze FEN {fen}: {e}")
        raise
    finally:
        if engine:
            print("Quitting Leela engine...")
            await asyncio.sleep(0.1)
            await engine.quit()
            print("Leela engine quit.")

async def get_system_metrics():
    # CPU Usage
    cpu_percent = psutil.cpu_percent(interval=None) # Non-blocking

    # RAM Usage
    ram = psutil.virtual_memory()
    ram_percent = ram.percent

    # NVIDIA GPU Usage (specifically for RTX 4060 on WSL)
    gpu_utilization = "N/A"
    gpu_memory_used = "N/A"
    gpu_memory_total = "N/A"
    gpu_temp = "N/A"

    try:
        smi_output = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=utilization.gpu,memory.used,memory.total,temperature.gpu", "--format=csv,noheader,nounits"],
            text=True,
        )
        parts = [p.strip() for p in smi_output.strip().split(',')]
        if len(parts) == 4:
            gpu_utilization = f"{parts[0]} %"
            gpu_memory_used = f"{parts[1]} MiB"
            gpu_memory_total = f"{parts[2]} MiB"
            gpu_temp = f"{parts[3]} C"
    except (subprocess.CalledProcessError, FileNotFoundError, IndexError) as e:
        pass

    return {
        "cpu_percent": cpu_percent,
        "ram_percent": ram_percent,
        "gpu_utilization": gpu_utilization,
        "gpu_memory_used": gpu_memory_used,
        "gpu_memory_total": gpu_memory_total,
        "gpu_temperature": gpu_temp
    }

# Semaphore to limit concurrent engine analysis calls if necessary.
# Set to 1 as Lc0 is usually best utilized with one analysis at a time per engine instance.
analysis_semaphore = asyncio.Semaphore(1)


async def analyze_single_position_with_semaphore(engine: chess.engine.UciProtocol, fen: str, n_nodes_limit:int,time_limit: float):
    async with analysis_semaphore:
        return await analyze_single_position(engine, fen, time_limit,n_nodes_limit)


async def analyze_fens_from_main_fen_batch(batch_size: int = 10000, n_nodes_limit:int=50_000, analysis_time_limit: float = 1.0):
    """
    Fetches unanalyzed FENs from main_fen in batches, analyzes them using Lc0,
    and inserts the results into the fen table.
    """
    fen_db_interface = DBInterface(Fen)

    start_time_total = time.time()
    total_fens_analyzed = 0

    engine = None
    try:
        engine = await initialize_lc0_engine()

        while True:
            metrics_before_fetch = await get_system_metrics()
            print(f"Metrics before fetching FENs: CPU={metrics_before_fetch['cpu_percent']}%, RAM={metrics_before_fetch['ram_percent']}%, GPU Util={metrics_before_fetch['gpu_utilization']}")
            print(f"Fetching {batch_size} unanalyzed FENs from main_fen...")
            fens_to_analyze = await open_async_request(
                """
                SELECT mf.fen
                FROM main_fen AS mf
                LEFT JOIN fen AS f ON mf.fen = f.fen
                WHERE f.fen IS NULL
                LIMIT :limit;
                """,
                params={"limit": batch_size}
            )
            fens_to_analyze_str = [f[0] for f in fens_to_analyze]

            if not fens_to_analyze_str:
                print("No more unanalyzed FENs found in main_fen. Exiting analysis loop.")
                break

            print(f"Retrieved {len(fens_to_analyze_str)} FENs for analysis.")

            # Map FENs to their tasks for easier error handling and identification
            analysis_tasks_map = {fen_str: analyze_single_position_with_semaphore(engine, fen_str, n_nodes_limit, analysis_time_limit)
                                  for fen_str in fens_to_analyze_str}

            fen_analysis_results = []
            start_time_batch = time.time()

            # Create a list of futures to await using asyncio.as_completed
            tasks_to_await = list(analysis_tasks_map.values())

            # Process tasks as they complete
            for completed_task in asyncio.as_completed(tasks_to_await):
                # Find the FEN associated with this task (reverse lookup)
                # This is a bit inefficient for very large batches, but safer for now.
                # A better way would be to pass the FEN *into* the coroutine itself.
                current_fen = "UNKNOWN_FEN" # Default for logging if lookup fails
                for f, t in analysis_tasks_map.items():
                    if t is completed_task: # Check if it's the same task object
                        current_fen = f
                        break

                try:
                    result: FenCreateData = await completed_task # Await the actual task
                    fen_analysis_results.append(result.model_dump())
                    total_fens_analyzed += 1
                except Exception as e:
                    # This catches any unhandled error from analyze_single_position_with_semaphore
                    # or if the task itself somehow failed without returning
                    print(f"CRITICAL: Task for FEN {current_fen} failed unexpectedly: {e}. "
                          f"Inserting a 'failed' record to prevent re-analysis loop.")
                    # Ensure a failed record is created and inserted for this FEN
                    failed_result = FenCreateData(
                        fen=current_fen,
                        depth=0,
                        seldepth=0,
                        time=0.0,
                        nodes=0,
                        score=-999999.0, # Indicate failed analysis
                        tbhits=0,
                        nps=0
                    )
                    fen_analysis_results.append(failed_result.model_dump())
                    total_fens_analyzed += 1 # Still count as processed for the purpose of breaking the loop


            if fen_analysis_results:
                print(f"Inserting {len(fen_analysis_results)} analysis results into 'fen' table...")
                insert_start_time = time.time()
                # The create_all method in DBInterface needs to handle ON CONFLICT DO NOTHING for Fen
                success = await fen_db_interface.create_all(fen_analysis_results)
                insert_end_time = time.time()
                if success:
                    print(f"Successfully inserted {len(fen_analysis_results)} FEN analysis results in {insert_end_time - insert_start_time:.2f} seconds.")
                else:
                    print(f"Failed to insert FEN analysis results for the batch.")
            else:
                print("No successful FEN analyses to insert in this batch.")

            end_time_batch = time.time()
            print(f"Batch analysis complete. Time taken for batch: {end_time_batch - start_time_batch:.2f} seconds.")
            metrics_after_batch = await get_system_metrics()
            print(f"Metrics after batch: CPU={metrics_after_batch['cpu_percent']}%, RAM={metrics_after_batch['ram_percent']}%, GPU Util={metrics_after_batch['gpu_utilization']}")

    except Exception as e:
        print(f"An error occurred during batch FEN analysis: {e}")
    finally:
        if engine:
            print("Quitting Leela engine...")
            await asyncio.sleep(0.1)
            await engine.quit()
            print("Leela engine quit.")


async def analyze_user_games_fens(
                                username: str,
                                n_games_to_process: int = 10,
                                analysis_time_limit: float = 3.0,
                                batch_size_fens: int = 100,
                                n_nodes_limit: int = 50_000
                            ):
    """
    Orchestrates the process of fetching new games for a user,
    generating FENs, updating main_fen, marking games as processed,
    and then analyzing the FENs using Lc0.
    """
    main_fen_db_interface = DBInterface(MainFen)
    fen_db_interface = DBInterface(Fen)
    processed_game_db_interface = DBInterface(ProcessedGame)

    total_fens_generated = 0
    total_games_processed = 0
    total_fens_analyzed = 0 # Initialize for this function's scope

    print(f"--- Starting FEN Analysis for User: {username} ---")

    # metrics_initial = await get_system_metrics()
    # print(f"Initial System Metrics: {metrics_initial}")

    while total_games_processed < n_games_to_process:
        # metrics_before_fetch = await get_system_metrics()
        # print(f"""Metrics before fetching game links: \n
        #         CPU={metrics_before_fetch['cpu_percent']}%,\n
        #         RAM={metrics_before_fetch['ram_percent']}%, \n
        #         GPU Util={metrics_before_fetch['gpu_utilization']}""")

        # 1. Fetch new game links for the user
        remaining_games = n_games_to_process - total_games_processed
        game_links_start_time = time.time()
        game_links = await get_game_links_by_username(username, remaining_games)
        game_links_end_time = time.time()
        print(f"Time to fetch {len(game_links)} NEW game links for user '{username}': {game_links_end_time - game_links_start_time:.4f} seconds")


        if not game_links:
            print(f"No new games found for user '{username}'. Stopping.")
            break

        print(f"Found {len(game_links)} new games for '{username}'.")
        current_games_batch_links = [link for link in game_links]

        # 2. Get moves for the batch of game links
        print(f"Fetching moves for {len(current_games_batch_links)} games...")
        games_moves = await get_all_moves_for_links_batch(current_games_batch_links)
        print(f"Successfully fetched moves for {len(games_moves)} games.")

        # 3. Generate FENs for these games and prepare for bulk upsert into main_fen
        print("Generating FENs from games...")
        all_new_main_fens_data = []
        processed_game_links_to_insert = []
        fen_generation_start_time = time.time()

        current_fens_generated_in_batch = 0
        for game_link, moves_data in games_moves.items():
            if not moves_data:
                print(f"Warning: No moves found for game link {game_link}. Skipping FEN generation for this game.")
                continue

            game_fens_with_counters = generate_fens_for_single_game_moves(moves_data)
            for fen_str, counters_dict in game_fens_with_counters:
                simplified_fen_data = simplify_fen_and_extract_counters_for_insert(fen_str, counters_dict)
                all_new_main_fens_data.append(simplified_fen_data)
            processed_game_links_to_insert.append({"link": game_link})
            current_fens_generated_in_batch += len(game_fens_with_counters)
            total_fens_generated += len(game_fens_with_counters)

        fen_generation_end_time = time.time()
        print(f"Generated {current_fens_generated_in_batch} unique FENs from user's NEW games (after validation): {fen_generation_end_time - fen_generation_start_time:.4f} seconds")

        if not all_new_main_fens_data:
            print("No FENs generated from this batch of games. Continuing to next batch of games if available.")
            if processed_game_links_to_insert:
                await processed_game_db_interface.create_all(processed_game_links_to_insert)
                total_games_processed += len(processed_game_links_to_insert)
            continue

        metrics_after_fen_gen = await get_system_metrics()
        print(f"System Metrics after FEN Generation: {metrics_after_fen_gen}")


        # 4. Insert/Update FENs into main_fen table (bulk upsert)
        print(f"--- Inserting/Updating {len(all_new_main_fens_data)} FENs into 'main_fen' ---")
        insert_main_fen_start_time = time.time()
        await insert_fens(all_new_main_fens_data) # This function should handle aggregation and upsert
        insert_main_fen_end_time = time.time()
        print(f"FEN insertion/update process complete.")
        print(f"Time to insert/update FENs in 'main_fen': {insert_main_fen_end_time - insert_main_fen_start_time:.4f} seconds")
        metrics_after_main_fen_insert = await get_system_metrics()
        print(f"System Metrics after Main FEN Insertion: {metrics_after_main_fen_insert}")


        # 5. Mark games as processed
        print(f"--- Marking {len(processed_game_links_to_insert)} games as processed ---")
        insert_processed_start_time = time.time()
        await insert_processed_game_links(processed_game_links_to_insert)
        insert_processed_end_time = time.time()
        print(f"Successfully inserted {len(processed_game_links_to_insert)} game links into processed_game.")
        print(f"Time to insert processed game links: {insert_processed_end_time - insert_processed_start_time:.4f} seconds")
        total_games_processed += len(processed_game_links_to_insert)
        metrics_after_processed_links_insert = await get_system_metrics()
        print(f"System Metrics after Processed Links Insertion: {metrics_after_processed_links_insert}")


        print(f"Starting analysis of {current_fens_generated_in_batch} FENs in batches using Lc0...")
        # 6. Analyze generated FENs from main_fen that are not yet in 'fen'
        # The total_fens_analyzed counter should be updated by analyze_fens_from_main_fen_batch
        await analyze_fens_from_main_fen_batch(batch_size_fens, n_nodes_limit, analysis_time_limit)

    final_metrics = {
        'status': 'completed',
        'username': username,
        'total_games_found': total_games_processed,
        'total_fens_generated': total_fens_generated,
        'total_fens_analyzed_successfully': total_fens_analyzed,
        'total_fens_with_errors': 0, # Assuming errors are now handled by marking as failed in DB
        'duration_seconds': time.time() - start_time_total
    }

    print(f"\n--- FEN Analysis for User {username} Complete ---")
    print(f"Total FENs generated: {total_fens_generated}")
    print(f"Total FENs analyzed successfully: {total_fens_analyzed}")
    print(f"Total FENs with analysis errors: 0")
    metrics_at_end = await get_system_metrics()
    print(f"Final System Metrics: {metrics_at_end}")
    print(f"Total Analysis Duration for user '{username}': {final_metrics['duration_seconds']:.4f} seconds")
    print(json.dumps(final_metrics, indent=2))
    return final_metrics


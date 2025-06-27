# OPERATIONS_FENS
import os
import chess
import chess.engine
import asyncio
import time # Import time for logging execution duration
import psutil # Import psutil for system resource monitoring
import subprocess # For calling external commands like nvidia-smi
import json # For parsing nvidia-smi output (if available)
import re # Import re for regular expressions
from typing import List, Dict, Any # Added for more specific type hints
import logging # Import logging module

# Configure logging for chess.engine to suppress verbose tracebacks
# This will prevent the InvalidMoveError traceback from appearing in the console
# for errors originating within the chess.engine module itself.
logging.getLogger("chess.engine").setLevel(logging.ERROR) # Changed back to ERROR for better visibility of engine issues
logging.getLogger("asyncio").setLevel(logging.ERROR) # Suppress asyncio internal logs only to ERROR, not CRITICAL
# Removed: logging.getLogger().setLevel(logging.ERROR) to allow other module logs

from constants import LC0_PATH, lc0_directory, LC0_WEIGHTS_FILE
from database.operations.models import FenCreateData
from database.database.db_interface import DBInterface # This now points to your async DBInterface
from database.database.models import Fen, MainFen # Ensure Fen is imported
from database.database.ask_db import open_async_request
# Corrected Import: generate_fens_for_single_game_moves is in collect_fens.py
from database.operations.collect_fens import generate_fens_for_single_game_moves,get_all_moves_for_links_batch, insert_fens, insert_processed_game_links, simplify_fen_and_extract_counters_for_insert


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
            cwd=lc0_directory,
            # Direct stderr to DEVNULL to suppress all engine-specific error messages
            # like "invalid uci (use 0000 for null moves): 'a1a1'" that bypass Python's logging.
            stderr=subprocess.DEVNULL 
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
            # Attempt to quit gracefully, but if it fails, just close the transport
            try:
                await engine_uci.quit()
            except Exception as quit_e:
                print(f"Error quitting Lc0 engine during init failure: {quit_e}. Attempting to close transport.")
                # If engine.quit() fails, force close the transport
                if hasattr(engine_uci, 'transport') and engine_uci.transport:
                    engine_uci.transport.close()
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
    
    # Define a reasonable timeout for single FEN analysis to prevent hangs
    # Adjust this value based on expected performance; 60 seconds is a starting point.
    ANALYSIS_TIMEOUT_SECONDS = 90 

    try:
        limit = chess.engine.Limit(nodes=nodes_limit, time=ANALYSIS_TIMEOUT_SECONDS) # Added time limit
        info = await engine_uci.analyse(board, limit=limit)
        
        # Ensure that score and pv are extracted safely
        score = info["score"].white().score(mate_score=10000) / 100
        
        pv = []
        if "pv" in info and info["pv"] is not None:
            for move_obj in info["pv"]:
                try:
                    # Lc0 might return unexpected move objects or even None in rare cases.
                    # Ensure it's a valid move object before calling .uci()
                    if isinstance(move_obj, chess.Move):
                        pv.append(move_obj.uci())
                    else:
                        logging.warning(f"Unexpected PV element type for FEN {fen}: {type(move_obj)}")
                except Exception as pv_error:
                    logging.warning(f"Error processing PV move for FEN {fen}: {move_obj}, Error: {pv_error}")

        # Prepare data for insertion
        fen_data_for_db = {
            'fen': fen,
            'depth': info.get('depth'),
            'seldepth': info.get('seldepth'),
            'time': info.get('time'),
            'nodes': info.get('nodes'),
            'score': score,
            'tbhits': info.get('tbhits', 0), # Default to 0 if not present
            'nps': info.get('nps', 0) # Default to 0 if nps is None
        }

        # Validate with Pydantic model (optional but good practice)
        fen_create_data = FenCreateData(**fen_data_for_db)
        
        fen_interface = DBInterface(Fen)
        # Await the create method, as DBInterface methods are now async
        await fen_interface.create(fen_create_data.model_dump())
        
        # Modify info to be returned
        info['fen'] = fen
        info['score'] = score
        info['pv'] = ''.join([move+'##' for move in pv])
        
        return info

    except chess.engine.Timeout: # Catch specific timeout error
        return {"fen": fen, "error": f"Lc0 Analysis Timeout after {ANALYSIS_TIMEOUT_SECONDS} seconds."}
    except chess.engine.EngineError as e:
        # Check if the error message is specifically about invalid UCI moves
        if "invalid uci" in str(e).lower():
            # Suppress console print for this specific, common error
            pass 
        else:
            # For other engine errors, print a general message (optional, as analyze_fens_from_main_fen_batch already logs)
            # print(f"Lc0 Engine Error for FEN {fen}: {e}")
            pass
        return {"fen": fen, "error": f"Lc0 Engine Error: {str(e)}"}
    except ValueError as e:
        # No print here, handled by batch function
        return {"fen": fen, "error": f"Result Parsing Error: {str(e)}"}
    except Exception as e:
        # No print here, handled by batch function
        return {"fen": fen, "error": f"Unexpected Analysis Error: {str(e)}"}


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
        engine = await initialize_lc0_engine() # Await engine initialization
        analysis_result = await analyze_single_position(engine, fen, nodes_limit=50000) # Await analysis
        return analysis_result
    except Exception as e:
        # Keep this print for top-level API calls for immediate feedback
        print(f"Error in analize_fen for FEN {fen}: {e}")
        return {"fen": fen, "error": f"Analysis failed: {e}"}
    finally:
        if engine:
            print(f"Quitting Lc0 engine for FEN {fen}...")
            # Ensure engine quits gracefully or transport is closed
            try:
                # Attempt to quit gracefully first
                await engine.quit()
            except Exception as e:
                # If quit() fails (e.g., process already dead/unresponsive), force close transport
                print(f"Error quitting Lc0 engine in analize_fen: {e}. Attempting to force close transport.")
                if hasattr(engine, 'transport') and engine.transport:
                    engine.transport.close()
            print(f"Lc0 engine quit for FEN {fen}.")


def get_system_metrics():
    """Fetches current CPU, RAM, and NVIDIA GPU usage (if nvidia-smi is available)."""
    metrics = {}
    # CPU Usage
    metrics['cpu_percent'] = psutil.cpu_percent(interval=None) # Non-blocking call
    # RAM Usage
    mem = psutil.virtual_memory()
    metrics['ram_percent'] = mem.percent
    metrics['ram_used_gb'] = round(mem.used / (1024**3), 2)
    metrics['ram_total_gb'] = round(mem.total / (1024**3), 2)

    # GPU Usage (NVIDIA specific via nvidia-smi, adjusted for WSL version)
    NVIDIA_SMI_PATH = '/usr/lib/wsl/lib/nvidia-smi' # Your identified path

    try:
        # Run nvidia-smi without query/format flags, as it doesn't support them in WSL version
        result = subprocess.run(
            [NVIDIA_SMI_PATH],
            capture_output=True, text=True, check=True
        )
        output_lines = result.stdout.splitlines()

        # Parse the output for RTX 4060 (Assuming it's GPU 0 as per your log)
        rtx_4060_found = False
        gpu_data_line = ""
        # Improved logic to find the specific GPU data line
        for i, line in enumerate(output_lines):
            # Look for the line containing the GPU name
            if "NVIDIA GeForce RTX 4060" in line:
                rtx_4060_found = True
                # The data line is usually the one immediately following the GPU name line
                if i + 1 < len(output_lines):
                    gpu_data_line = output_lines[i + 1]
                break # Found the GPU, no need to search further

        if rtx_4060_found and gpu_data_line:
            # Use regex to robustly extract values
            # Example line: |  0%   44C    P8            N/A  /  115W |    1672MiB /   8188MiB |     45%      Default |

            # Temperature: look for digits followed by 'C'
            temp_match = re.search(r'(\d+)C', gpu_data_line)
            metrics['gpu_temperature_celsius'] = int(temp_match.group(1)) if temp_match else "N/A"

            # Memory Usage: look for digitsMiB / digitsMiB
            mem_match = re.search(r'(\d+)MiB\s+/\s+(\d+)MiB', gpu_data_line)
            if mem_match:
                mem_used_mib = int(mem_match.group(1))
                mem_total_mib = int(mem_match.group(2))
                metrics['gpu_memory_used_mb'] = mem_used_mib
                metrics['gpu_memory_total_mb'] = mem_total_mib
                metrics['gpu_memory_used_gb'] = round(mem_used_mib / 1024, 2)
                metrics['gpu_memory_total_gb'] = round(mem_total_mib / 1024, 2)
            else:
                 metrics['gpu_memory_used_mb'] = "N/A"
                 metrics['gpu_memory_total_mb'] = "N/A"
                 metrics['gpu_memory_used_gb'] = "N/A"
                 metrics['gpu_memory_total_gb'] = "N/A"

            # GPU Utilization: look for digits% preceded by a space or pipe, and followed by a space
            # This regex targets the utilization percentage in the specific column structure.
            # It looks for a percentage that is part of the utilization string, typically
            # found in the latter parts of the line, after memory.
            util_match = re.search(r'(\d+)%\s+Default', gpu_data_line) # More precise match
            if util_match:
                metrics['gpu_utilization_percent'] = int(util_match.group(1))
            else:
                # Fallback to broader search if precise one fails, or set to N/A
                util_match_fallback = re.search(r'\|\s*(\d+)%', gpu_data_line)
                metrics['gpu_utilization_percent'] = int(util_match_fallback.group(1)) if util_match_fallback else "N/A"


        elif not rtx_4060_found:
            metrics['gpu_info'] = "NVIDIA GeForce RTX 4060 not found in nvidia-smi output."
        else: # If found but data_line is empty
            metrics['gpu_info'] = "Could not parse NVIDIA GeForce RTX 4060 data line."

    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        metrics['gpu_info'] = f"NVIDIA GPU not found or nvidia-smi error: {e}"
    except Exception as e: # Catch any parsing errors
        metrics['gpu_info'] = f"NVIDIA GPU metrics parsing error: {e}. Raw line: {gpu_data_line}"

    return metrics


async def analyze_fens_from_main_fen_batch(batch_size: int = 100, nodes_limit: int = 50000) -> List[dict]:
    """
    Fetches a batch of unanalyzed FENs from 'main_fen' table,
    analyzes them concurrently using Lc0, and stores results in 'fen' table.
    Logs execution times and system resource usage.

    Args:
        batch_size (int): The number of FENs to fetch and analyze in this batch.
        nodes_limit (int): The maximum number of nodes Lc0 should explore for each FEN.

    Returns:
        List[dict]: A list of analysis results for the processed FENs,
                    including any errors encountered for specific FENs.
    """
    start_total_time = time.time()
    print("\n--- Starting Batch FEN Analysis ---")
    print(f"Initial System Metrics: {get_system_metrics()}")

    engine = None
    analysis_results = []
    # Define a semaphore to limit concurrent analysis calls to the Lc0 engine
    # Starting with 1 to ensure commands are strictly sequential to the engine.
    # Adjust this value (e.g., 2, 4) to find optimal concurrency for your GPU.
    analysis_semaphore = asyncio.Semaphore(1) 
    
    try:
        # 1. Get unanalyzed FENs from the database
        start_db_fetch_time = time.time()
        fens_to_analyze = await get_unanalyzed_fens(limit=batch_size)
        db_fetch_duration = time.time() - start_db_fetch_time
        print(f"Time to fetch {len(fens_to_analyze)} unanalyzed FENs from DB: {db_fetch_duration:.4f} seconds")
        print(f"System Metrics after DB Fetch: {get_system_metrics()}")

        if not fens_to_analyze:
            print("No unanalyzed FENs found in 'main_fen' for this batch.")
            return []

        print(f"Starting analysis for {len(fens_to_analyze)} unanalyzed FENs using a semaphore...")
        
        # 2. Initialize Lc0 engine once for the batch
        start_engine_init_time = time.time()
        engine = None # Initialize engine to None before attempting to create
        try:
            engine = await initialize_lc0_engine()
        except Exception as e:
            print(f"Failed to initialize Lc0 engine for batch analysis: {e}")
            # If engine initialization fails, we cannot proceed with analysis
            return [{"error": f"Failed to initialize Lc0 engine: {e}"}]

        engine_init_duration = time.time() - start_engine_init_time
        print(f"Time to initialize Lc0 engine: {engine_init_duration:.4f} seconds")
        print(f"System Metrics after Engine Init: {get_system_metrics()}")


        # 3. Create a list of analysis tasks
        analysis_tasks = []
        for fen_str in fens_to_analyze:
            task = analyze_single_position_with_semaphore(engine, fen_str, nodes_limit, analysis_semaphore)
            analysis_tasks.append(task)
        
        # 4. Run analyses concurrently (limited by semaphore)
        start_analysis_run_time = time.time()
        results = await asyncio.gather(*analysis_tasks, return_exceptions=True)
        analysis_run_duration = time.time() - start_analysis_run_time
        print(f"Time for Lc0 analysis run for {len(fens_to_analyze)} FENs: {analysis_run_duration:.4f} seconds")
        print(f"System Metrics after Analysis Run: {get_system_metrics()}")


        # Process results
        for i, result in enumerate(results):
            fen_str = fens_to_analyze[i]
            if isinstance(result, Exception):
                # The analyze_single_position_with_semaphore now returns dict with error
                # so this 'if isinstance(result, Exception)' block might not be hit
                # unless a very critical, unexpected error propagates from asyncio.gather itself.
                print(f"Critical error during analysis of FEN {fen_str}: {result}")
                analysis_results.append({"fen": fen_str, "error": str(result)})
            else:
                analysis_results.append(result)
        
        print(f"Finished analysis for {len(analysis_results)} FENs.")
        return analysis_results

    except Exception as e:
        print(f"An unexpected error occurred during batch FEN analysis: {e}")
        return [{"error": f"Batch analysis failed: {e}"}]
    finally:
        if engine:
            start_engine_quit_time = time.time()
            print("Quitting Lc0 engine after batch analysis...")
            # Ensure engine quits gracefully or transport is closed
            try:
                # Give tasks a moment to finish any lingering operations,
                # e.g., releasing semaphore, before quitting the engine
                await asyncio.sleep(0.1) 
                # Attempt to quit gracefully first
                await engine.quit()
            except Exception as e:
                # If quit() fails (e.g., process already dead/unresponsive), force close transport
                print(f"Error quitting Lc0 engine: {e}. Attempting to force close transport.")
                if hasattr(engine, 'transport') and engine.transport:
                    engine.transport.close()
            engine_quit_duration = time.time() - start_engine_quit_time
            print(f"Time to quit Lc0 engine: {engine_quit_duration:.4f} seconds")
            print(f"Final System Metrics after Engine Quit: {get_system_metrics()}")
            print(f"Total Batch Analysis Duration: {time.time() - start_total_time:.4f} seconds")
            print("Lc0 engine quit.")

# Helper function to wrap analyze_single_position with a semaphore
async def analyze_single_position_with_semaphore(engine_uci: chess.engine.UciProtocol,
                                                 fen: str, nodes_limit: int,
                                                 semaphore: asyncio.Semaphore) -> dict:
    """
    Acquires the semaphore before calling analyze_single_position, releasing it afterwards.
    """
    async with semaphore:
        return await analyze_single_position(engine_uci, fen, nodes_limit)


async def get_main_fen_non_single_game_objects() -> List[Dict[str, Any]]:
    """
    Fetches FEN objects from 'main_fen' table where 'n_games' is not equal to 1.
    Returns:
        List[Dict[str, Any]]: A list of dictionaries, where each dictionary represents
                              a FEN object with 'fen', 'n_games', and 'moves_counter'.
    """
    sql_query = "SELECT fen, n_games, moves_counter FROM main_fen WHERE n_games > 2;" # Changed condition
    # Use fetch_as_dict=True to get results as dictionaries directly
    result = await open_async_request(sql_query, fetch_as_dict=True)
    return result

async def get_game_links_by_username(username: str, limit: int = 100) -> List[int]: # Added limit parameter
    """
    Fetches game links from the 'game' table for a given username,
    excluding those already marked as processed in 'processed_game_links',
    up to a specified limit.

    Args:
        username (str): The username to search for.
        limit (int): The maximum number of game links to fetch.

    Returns:
        List[int]: A list of new, unprocessed game links (integers) for the user.
    """
    sql_query = """
    SELECT g.link
    FROM game AS g
    LEFT JOIN processed_game AS pgl ON g.link = pgl.link
    WHERE (g.white = :username OR g.black = :username) AND pgl.link IS NULL
    LIMIT :limit; -- Added LIMIT clause
    """
    # Use open_async_request and await it. Fetch values directly as links.
    result_tuples = await open_async_request(sql_query, params={"username": username, "limit": limit})
    # Extract just the link (which is the first and only element in each tuple)
    return [link[0] for link in result_tuples]

async def analyze_user_games_fens(username: str, n_games:int= 10,batch_size: int = 100, nodes_limit: int = 50000) -> Dict[str, Any]:
    """
    Fetches games played by a specific user, generates FENs for those games,
    and then analyzes these FENs using Lc0, storing results in the 'fen' table.
    Crucially, it now also inserts/updates FENs into 'main_fen' and marks
    games as processed in 'processed_game_links' before analysis.
    The number of games fetched is limited by the 'batch_size' parameter.

    Args:
        username (str): The username whose games should be analyzed.
        batch_size (int): The number of FENs to process in each analysis batch.
                          This also acts as the limit for fetching new game links.
        nodes_limit (int): The maximum number of nodes Lc0 should explore for each FEN.

    Returns:
        Dict[str, Any]: A dictionary summarizing the analysis process,
                        including counts of FENs processed and any errors.
    """
    total_start_time = time.time()
    print(f"\n--- Starting FEN Analysis for User: {username} ---")
    print(f"Initial System Metrics: {get_system_metrics()}")

    try:
        # 1. Fetch game links for the specified user
        # This now only fetches games NOT yet in processed_game_links, up to 'batch_size'
        print(f"Fetching {batch_size} NEW game links for user '{username}'...")
        start_fetch_links = time.time()
        user_game_links = await get_game_links_by_username(username, limit=n_games) # Pass batch_size as limit
        fetch_links_duration = time.time() - start_fetch_links
        print(f"Time to fetch {len(user_game_links)} NEW game links for user '{username}': {fetch_links_duration:.4f} seconds")
        
        if not user_game_links:
            print(f"No NEW games found for user '{username}' to analyze.")
            return {"status": "completed", "message": f"No NEW games found for user '{username}' to analyze."}

        # 2. Generate FENs from these games
        print(f"Generating FENs from {len(user_game_links)} NEW games...")
        start_fen_gen = time.time()
        
        generated_fens = []
        all_moves_grouped = await get_all_moves_for_links_batch(user_game_links)

        for game_link in user_game_links:
            moves_for_game = all_moves_grouped.get(game_link)
            if moves_for_game:
                game_fens = generate_fens_for_single_game_moves(moves_for_game) # This function performs FEN validation
                generated_fens.extend(game_fens)
        
        unique_generated_fens = list(set(generated_fens)) # Ensure uniqueness
        fen_gen_duration = time.time() - start_fen_gen
        print(f"Generated {len(unique_generated_fens)} unique FENs from user's NEW games (after validation): {fen_gen_duration:.4f} seconds")
        print(f"System Metrics after FEN Generation: {get_system_metrics()}")

        if not unique_generated_fens:
            print(f"No valid FENs could be generated from NEW games for user '{username}'.")
            # Even if no FENs, we still want to mark the games as processed if they were fetched.
            # This is important to prevent re-fetching invalid games repeatedly.
            print(f"Marking {len(user_game_links)} games (that yielded no valid FENs) as processed...")
            start_processed_links_insert = time.time()
            links_for_processed_table = [(link,) for link in user_game_links]
            await insert_processed_game_links(links_for_processed_table)
            processed_links_insert_duration = time.time() - start_processed_links_insert
            print(f"Time to insert processed game links: {processed_links_insert_duration:.4f} seconds")
            return {"status": "completed", "message": f"No valid FENs generated for user '{username}'. {len(user_game_links)} games marked as processed."}


        # --- Insert/Update FENs into 'main_fen' table ---
        print(f"\n--- Inserting/Updating {len(unique_generated_fens)} FENs into 'main_fen' ---")
        start_main_fen_insert = time.time()
        # Convert unique_generated_fens (list of strings) to required format for insert_fens
        fens_for_main_fen_insert = [simplify_fen_and_extract_counters_for_insert(fen_str) for fen_str in unique_generated_fens]
        await insert_fens(fens_for_main_fen_insert)
        main_fen_insert_duration = time.time() - start_main_fen_insert
        print(f"Time to insert/update FENs in 'main_fen': {main_fen_insert_duration:.4f} seconds")
        print(f"System Metrics after Main FEN Insertion: {get_system_metrics()}")

        # --- Insert game links into 'processed_game_links' table ---
        print(f"\n--- Marking {len(user_game_links)} games as processed ---")
        start_processed_links_insert = time.time()
        links_for_processed_table = [(link,) for link in user_game_links]
        await insert_processed_game_links(links_for_processed_table)
        processed_links_insert_duration = time.time() - start_processed_links_insert
        print(f"Time to insert processed game links: {processed_links_insert_duration:.4f} seconds")
        print(f"System Metrics after Processed Links Insertion: {get_system_metrics()}")


        # 3. Analyze the generated FENs in batches (now that they are in main_fen and games are marked processed)
        print(f"\nStarting analysis of {len(unique_generated_fens)} FENs in batches using Lc0...")
        total_analyzed_count = 0
        total_errors_count = 0
        
        # Use a single engine instance for all batches within this function
        engine_for_user_analysis = None
        try:
            engine_for_user_analysis = await initialize_lc0_engine()
            analysis_semaphore = asyncio.Semaphore(1) # Semaphore to control concurrency to the single engine

            for i in range(0, len(unique_generated_fens), batch_size):
                fen_batch = unique_generated_fens[i:i + batch_size]
                print(f"Analyzing batch {i//batch_size + 1}/{(len(unique_generated_fens) + batch_size - 1)//batch_size} ({len(fen_batch)} FENs)...")
                
                batch_tasks = []
                for fen_str in fen_batch:
                    # Pass the *same* engine instance to each analysis task
                    task = analyze_single_position_with_semaphore(engine_for_user_analysis, fen_str, nodes_limit, analysis_semaphore)
                    batch_tasks.append(task)
                
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)

                for res in batch_results:
                    if "error" in res:
                        # Log error for individual FENs
                        print(f"Error for FEN {res.get('fen', 'unknown')}: {res['error']}")
                        total_errors_count += 1
                    else:
                        total_analyzed_count += 1
            
        except Exception as e:
            print(f"An unexpected error occurred during batch analysis for user '{username}': {e}")
            return {"status": "failed", "username": username, "error": str(e)}
        finally:
            if engine_for_user_analysis:
                try:
                    print("Quitting Lc0 engine after user game analysis...")
                    await engine_for_user_analysis.quit()
                    print("Lc0 engine quit.")
                except Exception as quit_e:
                    print(f"Error quitting Lc0 engine after user game analysis: {quit_e}. Attempting to force close transport.")
                    if hasattr(engine_for_user_analysis, 'transport') and engine_for_user_analysis.transport:
                        engine_for_user_analysis.transport.close()


        print(f"\n--- FEN Analysis for User {username} Complete ---")
        print(f"Total FENs generated: {len(unique_generated_fens)}")
        print(f"Total FENs analyzed successfully: {total_analyzed_count}")
        print(f"Total FENs with analysis errors: {total_errors_count}")
        print(f"Final System Metrics: {get_system_metrics()}")
        total_duration = time.time() - total_start_time
        print(f"Total Analysis Duration for user '{username}': {total_duration:.4f} seconds")

        return {
            "status": "completed",
            "username": username,
            "total_games_found": len(user_game_links), # This is now NEW games found
            "total_fens_generated": len(unique_generated_fens),
            "total_fens_analyzed_successfully": total_analyzed_count,
            "total_fens_with_errors": total_errors_count,
            "duration_seconds": total_duration
        }

    except Exception as e:
        print(f"An unexpected error occurred during analysis for user '{username}': {e}")
        return {"status": "failed", "username": username, "error": str(e)}


async def get_main_fen_non_single_game_objects() -> List[Dict[str, Any]]:
    """
    Fetches FEN objects from 'main_fen' table where 'n_games' is not equal to 1.
    Returns:
        List[Dict[str, Any]]: A list of dictionaries, where each dictionary represents
                              a FEN object with 'fen', 'n_games', and 'moves_counter'.
    """
    sql_query = "SELECT fen, n_games, moves_counter FROM main_fen WHERE n_games > 2;" # Changed condition
    # Use fetch_as_dict=True to get results as dictionaries directly
    result = await open_async_request(sql_query, fetch_as_dict=True)
    return result

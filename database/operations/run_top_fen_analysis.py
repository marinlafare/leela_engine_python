# run_top_fen_analysis.py

import time
import asyncio
import sys
import datetime
import os # <-- Import os

from database.database.engine import init_db, async_engine
from constants import CONN_STRING
from database.database.ask_db import *
from database.operations.fen import *
from database.database.db_interface import DBInterface
from database.database.models import AnalysisTimes

##############
# Variables for engine:
# n_repetitions = 3
# between_min = 4.0
# between_max = 600000.0
# limit = 50
# min_wait_time_seconds = 20
# max_wait_time = 1/5
# verbose_each = 100
# analyse_time_limit = 1.5
# nodes_limit = 100_000
# weights = "t1-256x10-distilled-swa-2432500.pb.gz"
# card = 0
##############

async def analize_repeated_fens_operations(n_repetitions:int = 3,
                                            between_min:int = 4.0,
                                            between_max:int = 600000.0,
                                            limit:int = 50,
                                            min_wait_time_seconds:int = 20,
                                            max_wait_time:int = 1/5,
                                            verbose_each:int = 10,
                                            analyse_time_limit:int = 1.5,
                                            nodes_limit:int = 100_000,
                                            weights:int = "t1-256x10-distilled-swa-2432500.pb.gz",
                                            card:int = 0,
                                          meta_repetitions:int = 0):
    print(f"[{datetime.datetime.now()}] Initializing database for run_top_fen_analysis.py...")
    await init_db(CONN_STRING)
    print(f"[{datetime.datetime.now()}] Database initialized for run_top_fen_analysis.py.")

    # Determine the project root directory more robustly
    # This assumes 'run_top_fen_analysis.py' is inside 'leela_engine_python/database/operations/'
    # So, go up two directories from the current script's location.
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_script_dir, '..', '..'))

    # The script to run, specified as its path relative to project_root
    script_relative_path = os.path.join('database', 'operations', 'default_gpu.py')
    
    start_all_batches = time.time()
    successful_runs = []
    
    print(f'[{datetime.datetime.now()}] Starting {n_repetitions} repetitions.')

    for step_index in range(n_repetitions):
        start_repetition = time.time()
        print(f'\n--- Repetition {step_index + 1} out of {n_repetitions} ---')

        command_args = [
            script_relative_path,
            f"--between_min={between_min}",
            f"--between_max={between_max}",
            f"--limit={limit}",
            f"--verbose_each={verbose_each}",
            f"--analyse_time_limit={analyse_time_limit}",
            f"--nodes_limit={nodes_limit}",
            f"--weights={weights}",
            f"--card={card}",
            f"--batch_index={step_index}",
            f"--n_batches={n_repetitions}"
        ]

        python_executable = sys.executable
        full_command = [python_executable] + command_args
        
        # Prepare the environment for the subprocess
        # We copy the current environment and add project_root to PYTHONPATH
        env = os.environ.copy()
        current_pythonpath = env.get('PYTHONPATH', '')
        # Add project_root to the PYTHONPATH if it's not already there
        if project_root not in current_pythonpath.split(os.pathsep):
            env['PYTHONPATH'] = project_root + os.pathsep + current_pythonpath if current_pythonpath else project_root
        
        print(f"[{datetime.datetime.now()}] Running external script: {' '.join(full_command)}")
        print(f"[{datetime.datetime.now()}] Subprocess CWD will be: {project_root}")
        print(f"[{datetime.datetime.now()}] Subprocess PYTHONPATH will include: {project_root}") # For debugging
        
        process = await asyncio.create_subprocess_exec(
            *full_command,
            cwd=project_root, # Set CWD
            env=env,          # Pass the modified environment with PYTHONPATH
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if stdout:
            print(f"[{datetime.datetime.now()}] Script stdout:\n{stdout.decode().strip()}")
        if stderr:
            decoded_stderr = stderr.decode().strip()
            if decoded_stderr: # Only print stderr if there's actual content
                print(f"[{datetime.datetime.now()}] Script stderr:\n{decoded_stderr}")
        
        print(f"[{datetime.datetime.now()}] Script '{script_relative_path}' completed with exit code {process.returncode}")
        successful_runs.append(process.returncode == 0)
        
        end_repetition = time.time()
        repetition_duration = end_repetition - start_repetition
        print(f'--- Repetition {step_index + 1} completed in {repetition_duration:.2f} seconds. ---')

        if step_index == n_repetitions - 1:
            actual_wait_time = max(min_wait_time_seconds, repetition_duration * max_wait_time)
            print(f"[{datetime.datetime.now()}] Waiting for {actual_wait_time:.2f} seconds before next repetition...")
            await asyncio.sleep(actual_wait_time)
            
    total_duration = time.time() - start_all_batches
    print(f"\n[{datetime.datetime.now()}] All {n_repetitions} repetitions completed in a total of {total_duration:.2f} seconds.")
    
    if async_engine:
        await async_engine.dispose()
        print(f"[{datetime.datetime.now()}] Main database engine disposed.")

    print(f"[{datetime.datetime.now()}] Main function finished. Successes: {successful_runs.count(True)}/{len(successful_runs)}")
    return "DONE"


# if __name__ == "__main__":
#     # Remove or adapt this dummy creation based on your actual default_gpu.py
#     # If your default_gpu.py is the one from the previous answer, ensure it's in the correct path.
#     default_gpu_file_path = os.path.join('database', 'operations', 'default_gpu.py')
#     import os
#     if not os.path.exists(default_gpu_file_path):
#         # # This part should ideally create a *valid* dummy for your test
#         # # For a full example, refer to the dummy content provided in the previous answer.
#         # # Just create a minimal one for ModuleNotFoundError test here.
#         # print(f"WARNING: '{default_gpu_file_path}' not found. Creating a minimal dummy.")
#         # os.makedirs(os.path.dirname(default_gpu_file_path), exist_ok=True)
#         # with open(default_gpu_file_path, "w") as f:
#         #     f.write("""
#         #             import sys; import time; import asyncio; 
#         #             from database.database.engine import init_db, async_engine, AsyncDBSession; 
#         #             from constants import CONN_STRING; 
#         #             async def run_test():
#         #                 try:
#         #                     print(f"[SUBPROCESS {time.ctime()}] Subprocess started. sys.path: {sys.path}")
#         #                     await init_db(CONN_STRING)
#         #                     print(f"[SUBPROCESS {time.ctime()}] DB initialized in subprocess.")
#         #                     await asyncio.sleep(0.1) # Simulate work
#         #                     print(f"[SUBPROCESS {time.ctime()}] Subprocess finished.")
#         #                     if async_engine: await async_engine.dispose()
#         #                 except Exception as e:
#         #                     print(f"[SUBPROCESS ERROR] {e}", file=sys.stderr)
#         #                     sys.exit(1)
#         #             asyncio.run(run_test())
#         #             """)
#         print('NO DEAFUL_GPU PATH ')
#         raise
#     asyncio.run(analize_repeated_fens_operations())
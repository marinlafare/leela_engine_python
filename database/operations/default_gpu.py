# database.operations.default_gpu.py
import time
import asyncio
import argparse # Import argparse for command-line arguments
import datetime # For AnalysisTimes timestamp

from database.database.engine import init_db, async_engine # Import async_engine for disposal
from constants import CONN_STRING # Ensure CONN_STRING is available
from database.database.ask_db import * # Assuming this brings in open_async_request etc.
from database.operations.fen import analize_most_repeated_fens, format_leela_results # Explicitly import functions
from database.database.db_interface import DBInterface
from database.database.models import AnalysisTimes, Fen # Import Fen model for update


async def save_analysis_times(batch_data):
    print('________ Saving Analysis Times ________')
    analysis_times_interface = DBInterface(AnalysisTimes)
    await analysis_times_interface.create(batch_data)
    print('_________ Analysis Times Saved _________')

async def run_default_gpu_analysis(
                    between_min: float,
                    between_max: float,
                    limit: int,
                    verbose_each: int,
                    analyse_time_limit: float,
                    nodes_limit: int,
                    weights: str,
                    card: int,
                    batch_index:int,
                    n_batches:int
                    ):
    
    default_start = time.time()

    # Ensure DB is initialized for this subprocess
    print("Initializing database for default_gpu.py...")
    await init_db(CONN_STRING)
    print("Database initialized for default_gpu.py.")

    between_tuple = (between_min, between_max)
    
    analize_results = await analize_most_repeated_fens(
        between=between_tuple,
        limit=limit,
        verbose_each=verbose_each,
        analyse_time_limit=analyse_time_limit,
        nodes_limit=nodes_limit,
        weights=weights,
        card = card
    )
    
    to_insert = await format_leela_results(analize_results)

    await DBInterface(Fen).update_fen_analysis_data(to_insert)
    batch_end = time.time() - default_start

    print(f'[{datetime.datetime.now()}] Batch {batch_index + 1} done in: {batch_end:.2f} seconds')
    print('%%%%%%%%%%%%%%..saving times data to db..%%%%%%%%%%%%%%%%%%%%')
    
    # Prepare data for AnalysisTimes
    analysis_data_to_save = {
        'card': card,
        'model': weights,
        'n_fens': limit,
        'batch_index': batch_index,
        'n_batches': n_batches,
        'time_elapsed': batch_end,
        'fens_per_second': limit / batch_end if batch_end > 0 else 0,
        'analyse_time_limit': analyse_time_limit,
        'nodes_limit': nodes_limit
    }
    await save_analysis_times(analysis_data_to_save)
    print('%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%')

    # Dispose the engine before exiting the subprocess
    if async_engine:
        await async_engine.dispose()
        print("Database engine disposed for default_gpu.py.")

    print(f"[{datetime.datetime.now()}] default_gpu.py finished for batch {batch_index + 1}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Leela chess analysis on FENs.")
    parser.add_argument('--between_min', type=float, default=2.0, help='Min value for "between" tuple.')
    parser.add_argument('--between_max', type=float, default=4.0, help='Max value for "between" tuple.')
    parser.add_argument('--limit', type=int, default=50, help='Number of FENs to analyze.')
    parser.add_argument('--verbose_each', type=int, default=100, help='Verbose output frequency.')
    parser.add_argument('--analyse_time_limit', type=float, default=1.5, help='Analysis time limit per FEN.')
    parser.add_argument('--nodes_limit', type=int, default=100_000, help='Nodes limit per FEN.')
    parser.add_argument('--weights', type=str, default="t1-256x10-distilled-swa-2432500.pb.gz", help='Weights file for Leela.')
    parser.add_argument('--card', type=int, default=0, help='GPU card ID to use.')
    parser.add_argument('--batch_index', type=int, required=True, help='Current batch index (0-based).')
    parser.add_argument('--n_batches', type=int, required=True, help='Total number of batches.')

    args = parser.parse_args()

    # Call the main async analysis function
    asyncio.run(run_default_gpu_analysis(
        between_min=args.between_min,
        between_max=args.between_max,
        limit=args.limit,
        verbose_each=args.verbose_each,
        analyse_time_limit=args.analyse_time_limit,
        nodes_limit=args.nodes_limit,
        weights=args.weights,
        card=args.card,
        batch_index=args.batch_index,
        n_batches=args.n_batches
    ))
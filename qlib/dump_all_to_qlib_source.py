from sqlalchemy import create_engine, text
import pymysql
import pandas as pd
import fire
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm


def dump_symbol(symbol, out_dir, engine, skip_exists=True):
    """Dump a single symbol to CSV file.
    
    Parameters
    ----------
    symbol : str
        Stock symbol to dump
    out_dir : str
        Output directory for CSV files
    engine : sqlalchemy.engine.Engine
        SQLAlchemy database engine
    skip_exists : bool
        Skip if file already exists
    """
    filename = os.path.join(out_dir, f'{symbol}.csv')
    if skip_exists and os.path.isfile(filename):
        return symbol, "skipped"
    
    try:
        # Use parameterized query to avoid SQL injection and to let pandas/SQLAlchemy
        # handle proper quoting/typing
        df = pd.read_sql(
            text("SELECT *, amount/volume*10 AS vwap FROM final_a_stock_eod_price WHERE symbol = :symbol"),
            engine,
            params={"symbol": symbol}
        )
        df.to_csv(filename, index=False)
        return symbol, "success"
    except Exception as e:
        return symbol, f"error: {str(e)}"


def dump_all_to_qlib_source(skip_exists=True, max_workers=16):
    """Dump per-symbol CSV files for qlib source.

    This version uses a SQLAlchemy engine (connectable) with pandas.read_sql and
    queries symbols in parallel using ThreadPoolExecutor to speed up processing.

    Parameters
    ----------
    skip_exists : bool
        Skip files that already exist
    max_workers : int
        Maximum number of worker threads for parallel processing (default: 16)
    """
    # Create engine with connection pool for thread safety
    engine = create_engine(
        'mysql+pymysql://root:@127.0.0.1/investment_data',
        pool_recycle=3600,
        pool_size=max_workers,
        max_overflow=max_workers * 2
    )

    script_path = os.path.dirname(os.path.realpath(__file__))
    out_dir = os.path.join(script_path, 'qlib_source')
    os.makedirs(out_dir, exist_ok=True)

    # Fetch distinct symbols
    symbols_df = pd.read_sql("SELECT DISTINCT symbol FROM final_a_stock_eod_price", engine)
    symbols = symbols_df['symbol'].tolist()
    
    print(f"Found {len(symbols)} symbols to process")
    print(f"Using {max_workers} worker threads")

    # Process symbols in parallel
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = {
            executor.submit(dump_symbol, symbol, out_dir, engine, skip_exists): symbol
            for symbol in symbols
        }
        
        # Process completed tasks with progress bar
        with tqdm(total=len(symbols), desc="Dumping symbols") as pbar:
            for future in as_completed(futures):
                symbol, status = future.result()
                results.append((symbol, status))
                pbar.update(1)
                if status.startswith("error"):
                    tqdm.write(f"Error processing {symbol}: {status}")

    # Print summary
    success_count = sum(1 for _, status in results if status == "success")
    skipped_count = sum(1 for _, status in results if status == "skipped")
    error_count = sum(1 for _, status in results if status.startswith("error"))
    
    print(f"\nSummary: {success_count} succeeded, {skipped_count} skipped, {error_count} errors")

    engine.dispose()


if __name__ == "__main__":
    fire.Fire(dump_all_to_qlib_source)

from sqlalchemy import create_engine, text
import pymysql
import pandas as pd
import fire
import os


def dump_all_to_qlib_source(skip_exists=True):
    """Dump per-symbol CSV files for qlib source.

    This version uses a SQLAlchemy engine (connectable) with pandas.read_sql and
    queries symbols one-by-one to avoid loading the entire table into memory and
    to ensure compatibility with pandas' supported connection types.
    """
    engine = create_engine('mysql+pymysql://root:@127.0.0.1/investment_data', pool_recycle=3600)

    script_path = os.path.dirname(os.path.realpath(__file__))
    out_dir = os.path.join(script_path, 'qlib_source')
    os.makedirs(out_dir, exist_ok=True)

    # Fetch distinct symbols and iterate per-symbol to reduce memory usage and
    # keep the DB connection usage compatible with pandas
    symbols_df = pd.read_sql("SELECT DISTINCT symbol FROM final_a_stock_eod_price", engine)
    symbols = symbols_df['symbol'].tolist()

    for symbol in symbols:
        filename = os.path.join(out_dir, f'{symbol}.csv')
        print("Dumping to file:", filename)
        if skip_exists and os.path.isfile(filename):
            continue
        # Use parameterized query to avoid SQL injection and to let pandas/SQLAlchemy
        # handle proper quoting/typing
        df = pd.read_sql(
            text("SELECT *, amount/volume*10 AS vwap FROM final_a_stock_eod_price WHERE symbol = :symbol"),
            engine,
            params={"symbol": symbol}
        )
        df.to_csv(filename, index=False)

    engine.dispose()


if __name__ == "__main__":
    fire.Fire(dump_all_to_qlib_source)

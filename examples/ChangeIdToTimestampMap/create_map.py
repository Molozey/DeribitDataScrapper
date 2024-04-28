import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd
from tqdm import tqdm
from joblib import Parallel, delayed

load_dotenv("../../.env")
HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_DATABASE", "postgres")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_USER = os.getenv("DB_USERNAME", "postgres")
DB_PWD = os.getenv("DB_PASSWORD", "postgres")


ENGINE = lambda: create_engine(
    f"mysql+mysqldb://{DB_USER}:{DB_PWD}@{HOST}:{DB_PORT}/{DB_NAME}"
)

take_each_n_row = 10_000
offset = 1_000_000


total_size = pd.read_sql(
    f"""SELECT MAX(CHANGE_ID) FROM DeribitOrderBook.TABLE_DEPTH_10""", ENGINE()
)

start_row = 0
number_of_batches = total_size.iloc[0, 0] // offset
start_batch = start_row // offset


def download_batch(batch_num: int):
    engine = ENGINE()
    left_pointer = batch_num * offset
    right_pointer = (batch_num + 1) * offset
    df = pd.read_sql(
        f"""
    SELECT * FROM DeribitOrderBook.TABLE_DEPTH_10
    WHERE CHANGE_ID between {left_pointer} and {right_pointer}
    AND CHANGE_ID mod {take_each_n_row} = 0;
    """,
        con=engine,
    )
    engine.dispose()
    return df


if __name__ == "__main__":
    results = Parallel(n_jobs=-1, verbose=0)(
        delayed(download_batch)(batch)
        for batch in tqdm(
            range(start_batch, number_of_batches),
            desc="Downloaded batches",
        )
    )

    big_df = pd.concat(results)
    big_df.reset_index(drop=True, inplace=True)
    time = big_df["TIMESTAMP_VALUE"]
    time_converted = pd.Series(
        [pd.Timestamp.fromtimestamp(_time // 1000) for _time in time]
    )
    big_df["TIMESTAMP"] = time_converted

    big_df[["CHANGE_ID", "TIMESTAMP"]].to_csv("TimeStampToIndexMap.csv", index=False)

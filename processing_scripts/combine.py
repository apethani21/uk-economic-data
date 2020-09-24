import os
import pandas as pd
from functools import reduce


def combine_data():
    data = pd.DataFrame()
    for file in os.listdir("../processed_data/"):
        if file == "uk_full_hpi.parquet":
            continue
        print("data shape:", data.shape, f". Merging {file}")
        df = (pd.read_parquet(f"../processed_data/{file}")
              .drop_duplicates()
              .dropna(how="all"))
        if data.empty:
            data = df.copy()
        else:
            data = data.merge(df, how="outer", left_index=True, right_index=True).drop_duplicates()
    data.to_parquet("../processed_data/combined.parquet")


if __name__ == "__main__":
    combine_data()

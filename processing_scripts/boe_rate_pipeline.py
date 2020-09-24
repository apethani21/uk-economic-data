import os
import pandas as pd


def process():
    os.makedirs("../processed_data", exist_ok=True)
    df = pd.read_csv("../raw_data/Bank Rate  Bank of England Database.csv")
    df["changed"] = True
    df.rename(columns={"Date Changed": "date", "Rate": "rate"}, inplace=True)
    df["date"] = pd.to_datetime(df["date"], dayfirst=True)
    df.sort_values(by="date", inplace=True)
    df.set_index("date", inplace=True, drop=True)
    df = df.reindex(pd.date_range(df.index[0], df.index[-1]))
    df.index.name = "date"
    df["rate"].ffill(inplace=True)
    df["changed"].fillna(False, inplace=True)
    df.to_parquet("../processed_data/boe_rate.parquet")


if __name__ == "__main__":
    process()
    print(os.path.basename(__file__), "done")

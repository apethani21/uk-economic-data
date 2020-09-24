import os
import re
import pandas as pd


def process():
    os.makedirs("../processed_data", exist_ok=True)
    df = pd.read_csv("../raw_data/UK-HPI-full-file-2017-01.csv")
    df.columns = df.columns.map(lambda x: x.replace("%", "Perc").replace("FTB", "Ftb").replace("FOO", "Foo"))
    df.columns = df.columns.map(lambda x: x[:-1] + x[-1].lower())
    df.columns = df.columns.map(lambda x: re.sub(r'(?<!^)(?=[A-Z])', '_', x).lower())
    df['date'] = pd.to_datetime(df['date'], dayfirst=False, yearfirst=False)
    df.set_index('date', inplace=True, drop=True)
    df.to_parquet("../processed_data/uk_full_hpi.parquet")


if __name__ == "__main__":
    process()
    print(os.path.basename(__file__), "done")

import pandas as pd
from pathlib import Path

# Path(__file__) finds the python scripts .parent finds the folder where those python scripts exist

PROJECT_DIR = Path(__file__).resolve().parent
INPUT_FILE = PROJECT_DIR / "sales_data_original.csv"
df = pd.read_csv(INPUT_FILE)

# 2. Fixed order (Region -> City) and added reset_index() for a clean DataFrame
df_by_region_city = df.groupby(["region", "city"])["sales"].sum().reset_index()

print(df_by_region_city)

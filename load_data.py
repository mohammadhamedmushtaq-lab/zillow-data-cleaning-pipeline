import pandas as pd
import os

def load_data(path):
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()
    return df

def load_all_data():
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DATA_DIR = os.path.join(BASE_DIR, "raw_data")

    files = {
        "df_1":"listing_mortgage_info.csv",
        "df_2":"listing_nearby_homes.csv",
        "df_3":"listing_price_history.csv",
        "df_4":"listing_schools_info.csv",
        "df_5":"listing_subtype.csv",
        "df_6":"listing_tax_info.csv",
        "df_7":"property_listings.csv"
    }

    dfs = {}

    for key, file in files.items():
        path = os.path.join(DATA_DIR, file)
        dfs[key] = load_data(path)

    return dfs


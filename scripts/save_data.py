import os

def save_data(dfs):
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    SAVE_DIR = os.path.join(BASE_DIR, "cleaned_data")

    os.makedirs(SAVE_DIR, exist_ok=True)

    file_names = {
        "df_1": "C_listing_mortgage_info.csv",
        "df_2": "C_listing_nearby_homes.csv",
        "df_3": "C_listing_price_history.csv",
        "df_4": "C_listing_schools_info.csv",
        "df_5": "C_listing_subtype.csv",
        "df_6": "C_listing_tax_info.csv",
        "df_7": "C_property_listings.csv"
    }

    for key, df in dfs.items():
        file_name = file_names.get(key, f"{key}.csv")
        path = os.path.join(SAVE_DIR, file_name)
        df.to_csv(path, index=False)
        print(f"Saved: {path}")
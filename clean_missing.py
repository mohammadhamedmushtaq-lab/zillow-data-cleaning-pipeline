from scripts.load_data import load_all_data

dfs = load_all_data()

def handle_missing(dfs):
    import numpy as np
    import pandas as pd

    df_1 = dfs["df_1"]
    df_2 = dfs["df_2"]
    df_3 = dfs["df_3"]
    df_4 = dfs["df_4"]
    df_5 = dfs["df_5"]
    df_6 = dfs["df_6"]
    df_7 = dfs["df_7"]

    # df_1
    df_2["livingAreaValueComp"] = df_2["livingAreaValueComp"].fillna(
    df_2["livingAreaValueComp"].median()
    )
    df_1["rate"] = df_1.groupby("bucketType")["rate"].transform(lambda x: x.fillna(x.median() if not x.isna().all() else df_1["col"].median()))
    df_1["rate"] = df_1["rate"].fillna(df_1["rate"].mean())

    # df_2
    df_2["livingAreaValueComp"] = df_2["livingAreaValueComp"].fillna(df_2["livingAreaValueComp"].median())
    df_2["lotAreaValueComp"] = df_2["lotAreaValueComp"].fillna(df_2["lotAreaValueComp"].median())
    df_2["livingAreaUnitsComp"] = df_2["livingAreaUnitsComp"].fillna("Square Feet")

    # df_3
    df_3["price"] = df_3["price"].replace(0, np.nan).fillna(df_3["price"].median())
    df_3["pricePerSquareFoot"] = df_3.groupby("event")["pricePerSquareFoot"].transform(lambda x: x.fillna(x.median() if not x.isna().all() else df_3["col"].median()))
    df_3["pricePerSquareFoot"] = df_3["pricePerSquareFoot"].fillna(df_3["pricePerSquareFoot"].median())
    df_3["source"] = df_3["source"].fillna("Unknown")

    # df_4
    df_4["schoolRating"] = df_4.groupby("type")["schoolRating"].transform(lambda x: x.fillna(x.median() if not x.isna().all() else df_4["col"].median()))
    df_4["schoolRating"] = df_4["schoolRating"].fillna(df_4["schoolRating"].median())
    df_4["grades"] = df_4["grades"].fillna("Unknown")

    # df_5
    df_5["lastUpdated"] = pd.to_datetime(df_5["lastUpdated"])

    # df_6
    df_6["propertyValue"] = df_6["propertyValue"].fillna(df_6["propertyValue"].median())
    df_6["taxPaid"] = df_6["taxPaid"].apply(lambda x: x if x >= 0 else np.nan)
    df_6["taxPaid"] = df_6["taxPaid"].fillna(df_6["taxPaid"].median())

    # df_7
    cols = ["price", "livingArea", "bedrooms", "bathrooms", "yearBuilt"]
    for col in cols:
        df_7[col] = df_7[col].replace(0, np.nan)

    df_7 = df_7.fillna({
        "livingArea": df_7["livingArea"].median(),
        "bedrooms": df_7["bedrooms"].median(),
        "bathrooms": df_7["bathrooms"].median(),
        "yearBuilt": df_7["yearBuilt"].median(),
        "county": "Unknown",
        "livingAreaUnits": "Square Feet"
    })

    df_7["timeOnZillow"] = df_7["timeOnZillow"].str.extract(r'(\d+)').astype(float)
    df_7["timeOnZillow"] = df_7["timeOnZillow"].fillna(df_7["timeOnZillow"].median())

    df_7 = df_7.drop(columns=["url", "streetAddress"])

    df_7["rentZestimate"] = df_7.groupby("city")["rentZestimate"].transform(
    lambda x: x.fillna(x.median())
    )
    df_7["rentZestimate"] = df_7["rentZestimate"].fillna(df_7["rentZestimate"].median())
    df_7["pageViewCount"] = df_7["pageViewCount"].fillna(df_7["pageViewCount"].median())
    df_7["favoriteCount"] = df_7["favoriteCount"].fillna(df_7["favoriteCount"].median())
    df_7["propertyTaxRate"] = df_7["propertyTaxRate"].fillna(df_7["propertyTaxRate"].median())

    df_7["datePosted"] = pd.to_datetime(df_7["datePosted"], errors="coerce")
    df_7["lastUpdated"] = pd.to_datetime(df_7["lastUpdated"], errors="coerce")

    # update dfs
    dfs["df_1"] = df_1
    dfs["df_2"] = df_2
    dfs["df_3"] = df_3
    dfs["df_4"] = df_4
    dfs["df_5"] = df_5
    dfs["df_6"] = df_6
    dfs["df_7"] = df_7

    return dfs

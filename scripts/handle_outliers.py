from scripts.load_data import load_all_data

dfs = load_all_data()

def handle_outliers(dfs):

    import numpy as np
    import pandas as pd

    df_1 = dfs["df_1"]
    df_2 = dfs["df_2"]
    df_3 = dfs["df_3"]
    df_4 = dfs["df_4"]
    df_5 = dfs["df_5"]
    df_6 = dfs["df_6"]
    df_7 = dfs["df_7"]

    # df_1 (IQR)
    q1 = df_1["rate"].quantile(0.25)
    q3 = df_1["rate"].quantile(0.75)
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    df_1 = df_1[(df_1["rate"] >= lower) & (df_1["rate"] <= upper)]

    # df_2
    upper = df_2["priceComp"].quantile(0.99)
    df_2 = df_2[(df_2["priceComp"] > 0) & (df_2["priceComp"] <= upper)].copy()    

    # df_3
    Q1 = df_3["pricePerSquareFoot"].quantile(0.25)
    Q3 = df_3["pricePerSquareFoot"].quantile(0.75)
    IQR = Q3 - Q1
    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR
    df_3 = df_3[
        (df_3["pricePerSquareFoot"] >= lower) &
        (df_3["pricePerSquareFoot"] <= upper)
    ].copy()

    # df_4 (aggregation)
    df_4 = df_4.groupby("zpid").agg({
        "schoolRating": "max",
        "distanceFromListing": "min"
    }).reset_index()

    # df_6
    df_6["propertyValue"] = df_6["propertyValue"].replace(0, np.nan)

    for col in ["valueIncreaseRate", "taxIncreaseRate"]:
        lower = df_6[col].quantile(0.01)
        upper = df_6[col].quantile(0.99)
        df_6 = df_6[(df_6[col] >= lower) & (df_6[col] <= upper)].copy()

    upper = df_6["taxPaid"].quantile(0.99)
    df_6 = df_6[df_6["taxPaid"] <= upper]
    df_6["taxPaid"] = df_6["taxPaid"].apply(lambda x: x if x >= 0 else np.nan)

    # df_7
    for col in ["bedrooms", "bathrooms", "livingArea"]:
        upper = df_7[col].quantile(0.99)
        df_7 = df_7[df_7[col] <= upper].copy()
    df_7 = df_7[(df_7["yearBuilt"] > 1800) & (df_7["yearBuilt"] < 2025)]

    upper = df_7["price"].quantile(0.99)
    df_7 = df_7[df_7["price"] <= upper].copy()

    # update
    dfs["df_1"] = df_1
    dfs["df_2"] = df_2
    dfs["df_3"] = df_3
    dfs["df_4"] = df_4
    dfs["df_5"] = df_5
    dfs["df_6"] = df_6
    dfs["df_7"] = df_7

    return dfs
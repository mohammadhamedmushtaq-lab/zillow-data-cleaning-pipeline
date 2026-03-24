from scripts.load_data import load_all_data

dfs = load_all_data()

def remove_duplicates(dfs):

    df_1 = dfs["df_1"]
    df_2 = dfs["df_2"]
    df_3 = dfs["df_3"]
    df_4 = dfs["df_4"]
    df_5 = dfs["df_5"]
    df_6 = dfs["df_6"]
    df_7 = dfs["df_7"]

    # df_1
    df_1 = df_1.drop_duplicates(subset=["zpid", "bucketType"])

    # df_2
    if "lastUpdated" in df_2.columns:
      df_2 = df_2.sort_values("lastUpdated").copy()
    df_2 = df_2.drop_duplicates(subset=["zpid", "zpidComp"], keep="last").copy()

    # df_3
    df_3 = df_3.drop_duplicates()

    # df_4
    df_4 = df_4.drop_duplicates(subset=["zpid", "schoolName"])

    # df_5
    df_5 = df_5.drop_duplicates()

    # df_6
    if "lastUpdatedDate" in df_6.columns:
       df_6 = df_6.sort_values("lastUpdatedDate").copy()
    df_6 = df_6.drop_duplicates(subset=["zpid"], keep="last").copy()

    # df_7
    df_7 = df_7.dropna(subset=["zpid", "price"]).copy()
    df_7 = df_7.drop_duplicates(subset=["zpid"]).copy()

    # update
    dfs["df_1"] = df_1
    dfs["df_2"] = df_2
    dfs["df_3"] = df_3
    dfs["df_4"] = df_4
    dfs["df_5"] = df_5
    dfs["df_6"] = df_6
    dfs["df_7"] = df_7

    return dfs
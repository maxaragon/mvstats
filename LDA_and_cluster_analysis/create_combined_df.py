from hashlib import sha512
import matplotlib.pyplot as plt
import seaborn as sn
import numpy as np
import pandas as pd
import glob
import os
from collections import defaultdict


def load_data():
    # Read state file and set index
    state = pd.read_excel("data/stateatlas_2011-2019.xlsx", index_col=0)
    
    # Read the topology file

    urban_rural = pd.read_excel("data/urban-rural-typology.xlsx", sheet_name=1, index_col=0)

    # Generate  a dataframe for every year in the open data (some excel files contain 2 years or more)

    open_11_12 = pd.read_excel("data/open date_2011-2012.xlsx", index_col=0) 
    open_11 = open_11_12.filter(regex="11$|GCD|GEM_NAME",axis=1)
    open_12 = open_11_12.filter(regex="12$|GCD|GEM_NAME",axis=1)

    open_13_14 = pd.read_excel("data/open date_2013-2014.xlsx", index_col=0)
    open_13 = open_13_14.filter(regex="13$|GCD|GEM_NAME",axis=1)
    open_14 = open_13_14.filter(regex="14$|GCD|GEM_NAME",axis=1)

    open_15_17 = pd.read_excel("data/open date_2015-2017.xlsx", index_col=0)
    open_15 = open_15_17.filter(regex="15$|GCD|GEM_NAME",axis=1)
    open_16 = open_15_17.filter(regex="16$|GCD|GEM_NAME",axis=1)
    open_17 = open_15_17.filter(regex="17$|GCD|GEM_NAME",axis=1)

    open_18 = pd.read_excel("data/open date_2018.xlsx", index_col=0)
    open_18 = open_18.filter(regex="18$|GCD|GEM_NAME",axis=1)

    open_19 = pd.read_excel("data/open date_2019.xlsx", index_col=0)
    open_19 = open_19.filter(regex="19$|GCD|GEM_NAME",axis=1)

    # Check number of columns in the "open" dataframes; Here the year 2014 is missing one column...

    print("Number of Columns in  2011: ", len(open_11.columns))
    print("Number of Columns in  2012: ", len(open_12.columns))
    print("Number of Columns in  2013: ", len(open_13.columns))
    print("Number of Columns in  2014: ", len(open_14.columns))
    print("Number of Columns in  2015: ", len(open_15.columns))
    print("Number of Columns in  2016: ", len(open_16.columns))
    print("Number of Columns in  2018: ", len(open_18.columns))
    print("Number of Columns in  2019: ", len(open_19.columns))

    # Generate a dataframe for every year in the state atlas data

    state_11 = state.filter(regex="11$|GCD|Name",axis=1)
    state_12 = state.filter(regex="12$|GCD|Name",axis=1)
    state_13 = state.filter(regex="13$|GCD|Name",axis=1)
    state_14 = state.filter(regex="14$|GCD|Name",axis=1)
    state_15 = state.filter(regex="15$|GCD|Name",axis=1)
    state_16 = state.filter(regex="16$|GCD|Name",axis=1)
    state_17 = state.filter(regex="17$|GCD|Name",axis=1)
    state_18 = state.filter(regex="18$|GCD|Name",axis=1)
    state_19 = state.filter(regex="19$|GCD|Name",axis=1)

    # Check number of columns in the "state" dataframes; Here the years 2013 and 2016 are missing one column each...

    print("in 2011: ", len(state_11.columns))
    print("in 2012: ", len(state_12.columns))
    print("in 2013: ", len(state_13.columns))
    print("in 2014: ", len(state_14.columns))
    print("in 2015: ", len(state_15.columns))
    print("in 2016: ", len(state_16.columns))
    print("in 2018: ", len(state_18.columns))
    print("in 2019: ", len(state_19.columns))

    # For consistency, let"s avoid these years: 2013, 2014, and 2016


    # Filter dataframes to only inlcude rural environments and merge with the "open" and "state" dfs per year


    rural_state_open_11 = pd.concat([urban_rural, state_11, open_11], axis=1, join="inner")
    rural_state_open_12 = pd.concat([urban_rural, state_12, open_12], axis=1, join="inner")
    rural_state_open_13 = pd.concat([urban_rural, state_13, open_13], axis=1, join="inner")
    rural_state_open_14 = pd.concat([urban_rural, state_14, open_14], axis=1, join="inner")
    rural_state_open_15 = pd.concat([urban_rural, state_15, open_15], axis=1, join="inner")
    rural_state_open_16 = pd.concat([urban_rural, state_16, open_16], axis=1, join="inner")
    rural_state_open_17 = pd.concat([urban_rural, state_17, open_17], axis=1, join="inner")
    rural_state_open_18 = pd.concat([urban_rural, state_18, open_18], axis=1, join="inner")
    rural_state_open_19 = pd.concat([urban_rural, state_19, open_19], axis=1, join="inner")


    dataframes_per_year = [
        ("2011", rural_state_open_11),
        ("2012", rural_state_open_12),
        #("2013", rural_state_open_13),
        #("2014", rural_state_open_14),
        ("2015", rural_state_open_15),
        #("2016", rural_state_open_16),
        ("2017", rural_state_open_17),
        ("2018", rural_state_open_18),
        ("2019", rural_state_open_19)
    ]

    combined_df = join_dataframe_years(dataframes_per_year)


    return combined_df


def join_dataframe_years(dataframes_per_year):
    new_dataframes = []

    for year, dataframe in dataframes_per_year:
        print(year)
        new_columns = defaultdict(list) 

        new_columns["GCD"] = dataframe.index
        new_columns["NAME"] = dataframe["Name"]
        new_columns["YEAR"] = year
        new_columns["RURAL_TYPE"] = dataframe["rural_type"]

        # new_columns["land_price"] = (dataframe["price_prop_19"] + dataframe["price_prop_20"]) / 2
        # new_columns["house_price"] = (dataframe["price_house_19"] + dataframe["price_house_20"]) / 2
        # new_columns["apartment_price"] = (dataframe["price_app_19"] + dataframe["price_app_20"]) / 2

        year_postfix = f"_{year[-2:]}"

        for column in dataframe:
            if column.endswith(year_postfix):
                new_column_name = column[:-3]
                new_columns[new_column_name] = dataframe[column].values

                # print(f"{column} -> {new_column_name}")

        df = pd.DataFrame.from_dict(new_columns)
        df.set_index(["GCD", "YEAR"], inplace=True)

        new_dataframes.append(df)

    combined_df = pd.concat(new_dataframes, axis=0, join="inner")

    print(combined_df.describe())
    print(combined_df.columns)

    return combined_df


def main():
    combined_df = load_data()
    combined_df.to_pickle("data/combined_df")

if __name__ == "__main__":
    main()

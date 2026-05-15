#import libraries 
import pandas as pd
import os

#Define name of columns that will be present in the final dataset
COLUMNS = [
    "date_processing",
    "year",
    "month",
    "state_dept",
    "victimization_fact",
    "sex",
    "ethnic_group",
    "age_range",
    "total_victim",
    "source"
]


def select_common_columns(df):
    """
    Standardizes the structure of a DataFrame to match the predefined COLUMNS list.

    This function ensures that all required columns are present in the input DataFrame.
    For each column in COLUMNS:
        - If the column does not exist in the DataFrame, it is created with empty values (pd.NA).
        - If the column exists, it is left unchanged.
    Finally, the DataFrame is returned with exactly the columns listed in COLUMNS,
    preserving the specified order.

    Returns:
        pd.DataFrame: A DataFrame containing all columns from COLUMNS, in the correct order,
        with missing columns filled with empty values.
    """

    for col in COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[COLUMNS]


def concat_sources(df1, df2):
    """
    Selects common columns from two DataFrames and concatenates them into one.

    Args:
        df1, df2 (pd.DataFrame): Input data sources.

    Returns:
        pd.DataFrame: Concatenated DataFrame.
    """
    df1 = select_common_columns(df1)
    df2 = select_common_columns(df2)
    return pd.concat([df1, df2], ignore_index=True)


def run_concat(source1_path, source2_path, output_path):
    """
     Entry point for Airflow task.
        Execute the merge process of multiple data sources.

        This function acts as an entry point for an Airflow task. It is responsible for:
        - Upload two datasets from parquet files.
        - Unify them using the `concat_sources`function.
        - Save the result in the specified path.
        - Show basic result information (number of rows and distribution by source).

        Args:
            source1_path (str): Path of the parquet file from source 1.
            source2_path (str): Path of the parquet file from source 2.
            output_path (str): The route where the final unified dataset will be saved.

        Returns:
            None
    """
    df1 = pd.read_parquet(source1_path)
    df2 = pd.read_parquet(source2_path)

    df_final = concat_sources(df1, df2)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_final.to_parquet(output_path, index=False)

    print(f"Concat complete: {len(df_final)} rows")
    print(f"Columns: {list(df_final.columns)}")
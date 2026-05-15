#Import necessary libraries
import pandas as pd
import unicodedata
import os


def remove_special_chars(text: str) -> str:
    """
    Removes accents and special characters from a string,
    returning the result in lowercase without leading or trailing spaces.

    Args:
        text (str): Input string to clean.

    Returns:
        str: Cleaned string in lowercase. If input is not a string, returns it unchanged.
    """
    if not isinstance(text, str):
        return text
    #Breaks down each character into base letter + accent (NFKD) and removes the accents
    nfkd = unicodedata.normalize("NFKD", text)
    cleaned = "".join(c for c in nfkd if not unicodedata.combining(c))
    return cleaned.lower().strip()


def normalize_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies remove_special_chars() to all text columns that contain
    uppercase letters or accents.

    Args:
        df (pd.DataFrame): Input DataFrame to normalize.

    Returns:
        pd.DataFrame: DataFrame with normalized text columns.
    """
    for col in df.select_dtypes(include="object").columns:
        #Only process if it contains at least one value with an uppercase letter or an 
        if df[col].astype(str).str.contains(r"[A-ZÁÉÍÓÚÑ]", regex=True).any():
            df[col] = df[col].apply(remove_special_chars)
    return df


def normalize_unknown_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Unifies all variants of missing or unknown values into a single
    standard label 'sin informacion' across all text columns.

    Args:
        df (pd.DataFrame): Input DataFrame to normalize.

    Returns:
        pd.DataFrame: DataFrame with standardized unknown values.
    """
    unknown_variants = [
        "no registra", "no informa", "sin informacion", "no informado",
        "no registrado", "nan", "none", "nd", "",
    ]
    for col in df.select_dtypes(include="object").columns:
        #fillna covers real NaN, truly empty cells
        df[col] = df[col].fillna("sin informacion")
        #replace covers strings that mean missing data
        df[col] = df[col].replace(unknown_variants, "sin informacion")
    return df


def normalize_ethnicity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Consolidates textual variants of the afrodescendiente ethnic group
    into the official label 'afrocolombiano'.

    Args:
        df (pd.DataFrame): Input DataFrame to normalize.

    Returns:
        pd.DataFrame: DataFrame with standardized ethnicity values.
    """
    if "ethnic_group" in df.columns:
        df["ethnic_group"] = df["ethnic_group"].replace(
            ["afrodescendiente", "negro", "moreno"], "afrocolombiano"
        )
    return df


def normalize_sex_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes sex column values to a consistent label format.

    Args:
        df (pd.DataFrame): Input DataFrame to normalize.

    Returns:
        pd.DataFrame: DataFrame with standardized sex values.
    """
    if "sex" in df.columns:
        #Replaces verbose labels with shorter standardized values
        df["sex"] = df["sex"].replace({"masculino": "hombre", "femenino": "mujer"})
    return df


def normalize_age_range(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replaces textual age range labels with standardized numeric intervals.

    Args:
        df (pd.DataFrame): Input DataFrame to normalize.

    Returns:
        pd.DataFrame: DataFrame with standardized age range values.
    """
    if "age_range" in df.columns:
        #Maps lifecycle labels to numeric intervals for consistency across sources
        df["age_range"] = df["age_range"].replace({
            "adulthood": "27-59", "youth": "14-26",
            "childhood": "0-13", "older adults": "60+",
        })
    return df


def prepare_for_groupby(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts key columns to the correct data types and extracts
    year and month as separate columns to enable grouping operations.

    Args:
        df (pd.DataFrame): Input DataFrame to prepare.

    Returns:
        pd.DataFrame: DataFrame ready for groupby operations.
    """
    if "vulnerability_index" in df.columns:
        #Converts to numeric; invalid values become NaN instead of raising an error
        df["vulnerability_index"] = pd.to_numeric(df["vulnerability_index"], errors="coerce")
    if "date_processing" in df.columns:
        df["date_processing"] = pd.to_datetime(df["date_processing"], errors="coerce")
        df["year"] = df["date_processing"].dt.year.astype("Int64")
        df["month"] = df["date_processing"].dt.month.astype("Int64")
    return df


def group_and_aggregate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Groups the DataFrame by key demographic and geographic columns,
    counting the number of records per group as total victims.

    Args:
        df (pd.DataFrame): Input DataFrame to group and aggregate.

    Returns:
        pd.DataFrame: Aggregated DataFrame with total victims and data quality flag per group.
    """
    group_cols = [
        "sex", "ethnic_group", "age_range", "victimization_fact",
        "date_processing", "year", "month", "state_dept", "source",
    ]
    group_cols = [c for c in group_cols if c in df.columns]
    #Filters out columns that are not present in the DataFrame
    df = (
        df.groupby(group_cols, dropna=False)
        .size()
        .reset_index(name="total_victim")
    )
    #Groups by all available columns and counts records per group
    df["total_victim_flag"] = df["total_victim"].apply(
        lambda x: "sin informacion" if pd.isna(x) else "reportado"
    )
    return df


def cast_categories(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts key categorical columns to the 'category' dtype
    to reduce memory usage compared to plain strings.

    Args:
        df (pd.DataFrame): Input DataFrame to cast.
        
    Returns:
        pd.DataFrame: DataFrame with categorical columns properly typed.
    """
    for col in ["sex", "ethnic_group", "age_range", "victimization_fact", "state_dept", "source", "total_victim_flag"]:
        if col in df.columns:
            df[col] = df[col].astype("category")
    return df


def transform_source1(input_path: str, output_path: str):
    """
    Orchestrates the full transformation pipeline for source 1 (Cali - Valle del Cauca).
    Loads raw data, applies all normalization and aggregation steps 
    and saves the result as Parquet.

    Args:
        input_path (str): Path to the raw input Parquet file.
        output_path (str): Destination path for the transformed Parquet file.
    """
    print("Loading source 1...")
    df = pd.read_parquet(input_path)
    print(f"Rows before transform: {len(df)}")

    #Tags every record with its source and department for traceability
    df["source"] = "cali"
    df["state_dept"] = "valle del cauca"

    #Renames classification to age_range to match the standard column name
    if "classification" in df.columns:
        df = df.rename(columns={"classification": "age_range"})
    if "total_victim" in df.columns:
        df = df.drop(columns=["total_victim"])

    #Applies the full transformation pipeline
    df = normalize_text_columns(df)
    df = normalize_unknown_values(df)
    df = normalize_ethnicity(df)
    df = normalize_sex_column(df)
    df = normalize_age_range(df)
    df = prepare_for_groupby(df)
    df = group_and_aggregate(df)
    df = cast_categories(df)

    print(f"Rows after transform: {len(df)}")
    print(f"Columns: {list(df.columns)}")
    #Creates output directory if it doesn't exist and saves the result
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"Saved to {output_path}")
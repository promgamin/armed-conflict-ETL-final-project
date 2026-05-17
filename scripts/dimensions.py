import os
import pandas as pd


def make_dim(df: pd.DataFrame, cols: list[str], id_col: str) -> pd.DataFrame:
    """
    Builds a dimension table from the given columns.
    Deduplicates, resets index and adds a surrogate integer key.

    Args:
        df      : Source consolidated DataFrame.
        cols    : Columns that define the dimension (natural keys).
        id_col  : Name for the surrogate key column (e.g. 'id_person').

    Returns:
        pd.DataFrame with id_col as first column followed by cols.
    """
    dim = df[cols].drop_duplicates().reset_index(drop=True)
    dim.insert(0, id_col, range(1, len(dim) + 1))
    return dim


def build_dimensions(input_path: str, output_dir: str):
    """
    Reads the consolidated dataset and writes one parquet per dimension
    table plus the resolved fact table.

    Files written to output_dir:
        dim_person.parquet
        dim_act.parquet
        dim_location.parquet
        dim_date.parquet
        fact_victims.parquet

    Args:
        input_path : Path to dataset_consolidated.parquet.
        output_dir : Directory where dimension parquets will be saved.
    """
    print("Reading consolidated dataset...")
    df = pd.read_parquet(input_path)
    print(f"Rows: {len(df)}")

    os.makedirs(output_dir, exist_ok=True)

# Dimension tables
    dim_person = make_dim(df, ["sex", "ethnic_group", "age_range"], "id_person")
    dim_act    = make_dim(df, ["victimization_fact"], "id_act")
    dim_location = make_dim(df, ["state_dept"], "id_location")

# Date dimension: one row per unique date_processing value
    dim_date = (
        df[["date_processing", "year", "month"]]
        .drop_duplicates(subset=["date_processing"])
        .reset_index(drop=True)
    )

# Resolve surrogate keys into the fact table 
    fact = df.copy()
    fact = fact.merge(dim_person,   on=["sex", "ethnic_group", "age_range"], how="left")
    fact = fact.merge(dim_act,      on=["victimization_fact"],               how="left")
    fact = fact.merge(dim_location, on=["state_dept"],                       how="left")

# Keep only the columns that belong in the fact table
    fact = fact[[
        "id_person",
        "id_act",
        "id_location",
        "date_processing",
        "total_victim",
        "source",
    ]]

# Drop rows where any FK is null (referential integrity)
    before = len(fact)
    fact = fact.dropna(subset=["id_person", "id_act", "id_location", "date_processing"])
    dropped = before - len(fact)
    if dropped:
        print(f"Warning: {dropped} rows dropped due to null foreign keys")

# Save parquets 
    dim_person.to_parquet(  f"{output_dir}/dim_person.parquet",   index=False)
    dim_act.to_parquet(     f"{output_dir}/dim_act.parquet",      index=False)
    dim_location.to_parquet(f"{output_dir}/dim_location.parquet", index=False)
    dim_date.to_parquet(    f"{output_dir}/dim_date.parquet",     index=False)
    fact.to_parquet(        f"{output_dir}/victims.parquet", index=False)

    print(f"dim_person    : {len(dim_person)} rows")
    print(f"dim_act       : {len(dim_act)} rows")
    print(f"dim_location  : {len(dim_location)} rows")
    print(f"dim_date      : {len(dim_date)} rows")
    print(f"victims  : {len(fact)} rows")
    print("Constructed dimensions")
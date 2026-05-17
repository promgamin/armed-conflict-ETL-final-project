#import libraries 
import pandas as pd
import unicodedata
import os


def remove_special_chars(text: str) -> str:
    """
    Cleans a text string by removing accents and special characters.

    Convert the text using NFKD to a basic form (no tildes or symbols),
    moves it to lowercase and removes spaces at the beginning and end.

    Args:
        text (str): Text you want to clean.

    Returns:
        str: Clean text in lower case and no accents.
             If the input is not a string, returns the original value.
    """

    if not isinstance(text, str):
        return text
    nfkd = unicodedata.normalize("NFKD", text)
    cleaned = "".join(c for c in nfkd if not unicodedata.combining(c))
    return cleaned.lower().strip()


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalizes the column names of a DataFrame.

    Apply remove_special_chars to each column name and replace
    the low-script spaces.

    Args:
        df (pd.DataFrame): DataFrame with columns to normalize.

    Returns:
        pd.DataFrame: DataFrame with normalized column names.
    """
    df.columns = [remove_special_chars(col).replace(" ", "_") for col in df.columns]
    return df


def normalize_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalizes the content of all text (object) columns.

    Apply remove_special_chars to each cell in the categorical columns
    or the DataFrame text.

    Args:
        df (pd.DataFrame): DataFrame with text columns to normalize.

    Returns:
        pd.DataFrame: DataFrame with normalized text values.
    """

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].apply(remove_special_chars)
    return df


def normalize_unknown_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize unknown or null values in text columns.

    Replace common variants of missing or unreported values
    (as 'no registra', 'nd', NaN, etc.) by the unified label
    'sin informacion' in all object type columns.

    Args:
        df (pd.DataFrame): DataFrame with possible unknown values.

    Returns:
        pd.DataFrame: Data frame with unknown values standardized.
    """

    unknown_variants = [
        "no registra", "no informa", "sin informacion", "no informado",
        "no registrado", "sin definir", "nan", "none", "nd", "",
    ]
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].fillna("sin informacion")
        df[col] = df[col].replace(unknown_variants, "sin informacion")
    return df


def normalize_departments(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalizes department codes and names for Colombia.

    Performs the following operations:
    - Standardizes cod_estado_depto format to two digits with leading zeros.
    - Marks invalid or empty codes as 'sin informacion'.
    - Infers the code from state_dept when missing, and vice versa.
    - Invalidates codes or names not found in the official catalog.

    Args:
        df (pd.DataFrame): DataFrame with 'cod_estado_depto' and/or 'state_dept' columns.

    Returns:
        pd.DataFrame: DataFrame with normalized and consistent department data.
    """

    # Official department catalogue: DANE code -> standardized name
    departamentos = {
        "91": "amazonas", "05": "antioquia", "81": "arauca", "08": "atlantico",
        "11": "bogota, d.c.", "13": "bolivar", "15": "boyaca", "17": "caldas",
        "18": "caqueta", "85": "casanare", "19": "cauca", "20": "cesar",
        "27": "choco", "23": "cordoba", "25": "cundinamarca", "94": "guainia",
        "95": "guaviare", "41": "huila", "44": "la guajira", "47": "magdalena",
        "50": "meta", "52": "narino", "54": "norte de santander", "86": "putumayo",
        "63": "quindio", "66": "risaralda", "88": "san andres", "68": "santander",
        "70": "sucre", "73": "tolima", "76": "valle del cauca", "97": "vaupes", "99": "vichada",
    }

    # Reverse catalog: name -> DANE code
    dept_to_code = {v: k for k, v in departamentos.items()}

    # Normalize cod_estado_depto: fill in with zeros and invalidate blank values
    if "cod_estado_depto" in df.columns:
        df["cod_estado_depto"] = df["cod_estado_depto"].astype(str).str.zfill(2)
        df.loc[df["cod_estado_depto"].isin(["00", "0", "sin informacion", "nan", "none"]), "cod_estado_depto"] = "sin informacion"

    # Standardize unknown values in state_dept
    if "state_dept" in df.columns:
        df.loc[df["state_dept"].isin(["sin definir", "sin informacion", "nan", "none"]), "state_dept"] = "sin informacion"

    if "cod_estado_depto" in df.columns and "state_dept" in df.columns:
        # Infer code from name when code is missing
        mask = df["cod_estado_depto"].eq("sin informacion") & df["state_dept"].ne("sin informacion")
        df.loc[mask, "cod_estado_depto"] = df.loc[mask, "state_dept"].map(dept_to_code)
        # Infer name from code when name is missing
        mask = df["state_dept"].eq("sin informacion") & df["cod_estado_depto"].ne("sin informacion")
        df.loc[mask, "state_dept"] = df.loc[mask, "cod_estado_depto"].map(departamentos)

    # Invalidate codes that do not belong to the official catalog
    if "cod_estado_depto" in df.columns:
        df.loc[~df["cod_estado_depto"].isin(list(departamentos.keys()) + ["sin informacion"]), "cod_estado_depto"] = "sin informacion"

    # Invalidate names that do not belong to the official catalogue
    if "state_dept" in df.columns:
        df.loc[~df["state_dept"].isin(list(departamentos.values()) + ["sin informacion"]), "state_dept"] = "sin informacion"

    return df


def normalize_ethnicity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes ethnic group categories in the 'ethnic_group' column.

    Consolidates equivalent text variants (including RA accreditation mentions)
    into canonical labels: 'afrocolombiano', 'indigena', 'rom', 'raizal' and 'palenquero'.

    Args:
        df (pd.DataFrame): DataFrame containing the 'ethnic_group' column.

    Returns:
        pd.DataFrame: DataFrame with normalized ethnic group values.
    """

    if "ethnic_group" in df.columns:
        df["ethnic_group"] = df["ethnic_group"].replace({
            "afrocolombiano (acreditado ra)": "afrocolombiano",
            "negro(a) o afrocolombiano(a)": "afrocolombiano",
            "negro (acreditado ra)": "afrocolombiano",
            "indigena": "indigena", "indigena (acreditado ra)": "indigena",
            "gitano(a) rom": "rom", "gitano (rrom) (acreditado ra)": "rom",
            "raizal del archipielago de san andres y providencia": "raizal",
            "palenquero (acreditado ra)": "palenquero", "palenquero": "palenquero"
        })
    return df


def normalize_age_range(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes the age range format in the 'age_range' column.

    Removes the word 'entre', replaces 'y' with '-' as separator and strips
    whitespace, leaving the range in a compact format (e.g.: '18-28').

    Args:
        df (pd.DataFrame): DataFrame containing the 'age_range' column.

    Returns:
        pd.DataFrame: DataFrame with normalized age range values.
    """

    if "age_range" in df.columns:
        df["age_range"] = df["age_range"].astype(str)
        df["age_range"] = df["age_range"].str.replace("entre", "", regex=False)
        df["age_range"] = df["age_range"].str.replace("y", "-", regex=False)
        df["age_range"] = df["age_range"].str.replace(" - ", "-", regex=False)
        df["age_range"] = df["age_range"].str.strip()
    return df


def prepare_for_groupby(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepares a DataFrame for aggregation by cleaning and transforming key columns.

    - Converts 'date_processing' to datetime and extracts year and month.
    - Ensures 'total_victim' is numeric.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: Transformed DataFrame ready for grouping.
    """
    if "date_processing" in df.columns:
        df["date_processing"] = pd.to_datetime(
            df["date_processing"], format="mixed", dayfirst=True, errors="coerce"
        )
        df["year"] = df["date_processing"].dt.year.astype("Int64")
        df["month"] = df["date_processing"].dt.month.astype("Int64")
    if "total_victim" in df.columns:
        df["total_victim"] = pd.to_numeric(df["total_victim"], errors="coerce")
    return df


def group_and_aggregate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Groups the DataFrame by available dimensions and aggregates total victims.

    - Dynamically selects grouping columns if present.
    - Sums 'total_victim' and adds a flag indicating data availability.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: Aggregated DataFrame with total victims and status flag.
    """
    group_cols = [
        "date_processing", "year", "month", "victimization_fact",
        "sex", "ethnic_group", "age_range", "state_dept", "source",
    ]
    group_cols = [c for c in group_cols if c in df.columns]

    df = (
        df.groupby(group_cols, dropna=False)
        .agg(total_victim=("total_victim", "sum"))
        .reset_index()
    )

    df["total_victim_flag"] = df["total_victim"].apply(
        lambda x: "sin informacion" if pd.isna(x) else "reportado"
    )
    return df


def cast_categories(df: pd.DataFrame) -> pd.DataFrame:
    """
    Casts selected columns to categorical data type if they exist.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with specified columns converted to category dtype.
    """
    for col in ["state_dept", "sex", "ethnic_group", "age_range", "victimization_fact", "source", "total_victim_flag"]:
        if col in df.columns:
            df[col] = df[col].astype("category")
    return df


def transform_source2(input_path: str, output_path: str):
    """
     The function loads a parquet file, normalizes column names and values,
    standardizes key variables, prepares the data for aggregation, performs
    grouping, and casts data types. Finally, it saves the processed output
    to the specified path.

    Args:
        input_path (str): Path to the input parquet file.
        output_path (str): Path where the transformed file will be saved.

    Returns:
        None
    """
    print("Loading source 2...")
    df = pd.read_parquet(input_path)
    print(f"Rows before transform: {len(df)}")

    df = normalize_column_names(df)
    # Rename columns to the standard project schema
    df = df.rename(columns={
        "fecha_corte": "date_processing", "estado_depto": "state_dept",
        "sexo": "sex", "hecho": "victimization_fact", "etnia": "ethnic_group",
        "ciclo_vital": "age_range", "per_ocu": "total_victim",
    })

    df["source"] = "nacional (colombia)"

    df = normalize_text_columns(df)
    df = normalize_unknown_values(df)
    df = normalize_departments(df)
    df = normalize_ethnicity(df)
    df = normalize_age_range(df)
    df = prepare_for_groupby(df)
    df = group_and_aggregate(df)
    df = cast_categories(df)

    print(f"Rows after transform: {len(df)}")
    print(f"Columns: {list(df.columns)}")

    # Create output directory if it doesn’t exist and save result
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"Saved to {output_path}")
import pandas as pd
import pymysql

DB_CONFIG = {
    "host": "mysql_dw",
    "port": 3306,
    "user": "etl_user",
    "password": "etl_password",
    "database": "dw_armed_conflict",
    "charset": "utf8mb4",
}

DDL = [
    """CREATE TABLE IF NOT EXISTS person (
        id_person    INT PRIMARY KEY NOT NULL,
        sex          VARCHAR(100),
        ethnic_group VARCHAR(100),
        age_range    VARCHAR(100)
    )""",
    """CREATE TABLE IF NOT EXISTS victimizing_act (
        id_act             INT PRIMARY KEY NOT NULL,
        victimization_fact VARCHAR(100)
    )""",
    """CREATE TABLE IF NOT EXISTS location (
        id_location INT PRIMARY KEY NOT NULL,
        state_dept  VARCHAR(100)
    )""",
    """CREATE TABLE IF NOT EXISTS registration_date (
        date_processing DATE PRIMARY KEY NOT NULL,
        year            INT,
        month           INT
    )""",
    """CREATE TABLE IF NOT EXISTS victims (
        id_person       INT,
        id_act          INT,
        id_location     INT,
        date_processing DATE,
        total_victim    INT,
        source          VARCHAR(100),
        CONSTRAINT fk_person   FOREIGN KEY (id_person)       REFERENCES person(id_person),
        CONSTRAINT fk_act      FOREIGN KEY (id_act)          REFERENCES victimizing_act(id_act),
        CONSTRAINT fk_location FOREIGN KEY (id_location)     REFERENCES location(id_location),
        CONSTRAINT fk_date     FOREIGN KEY (date_processing) REFERENCES registration_date(date_processing)
    )""",
]


def _insert_dataframe(cur, table: str, df: pd.DataFrame):
    """
    Generic helper: inserts all rows of df into table using INSERT IGNORE.
    Column order in df must match the CREATE TABLE definition.

    Args:
        cur   : Active pymysql cursor.
        table : Target table name.
        df    : DataFrame whose columns match the table columns exactly.
    """
    placeholders = ", ".join(["%s"] * len(df.columns))
    sql = f"INSERT IGNORE INTO {table} VALUES ({placeholders})"
    rows = [
        tuple(None if pd.isna(v) else v for v in row)
        for row in df.itertuples(index=False, name=None)
    ]
    cur.executemany(sql, rows)


def load_to_mysql(processed_dir: str):
    """
    Loads pre-built dimension and fact parquets into MySQL.
    No transformations are performed here — only INSERT statements.

    Args:
        processed_dir : Directory containing the parquet files produced
                        by build_dimensions.py.
    """
    print("Reading parquet files...")
    dim_person   = pd.read_parquet(f"{processed_dir}/dim_person.parquet")
    dim_act      = pd.read_parquet(f"{processed_dir}/dim_act.parquet")
    dim_location = pd.read_parquet(f"{processed_dir}/dim_location.parquet")
    dim_date     = pd.read_parquet(f"{processed_dir}/dim_date.parquet")
    fact         = pd.read_parquet(f"{processed_dir}/victims.parquet")

    # Fix types before inserting
    dim_date["date_processing"] = pd.to_datetime(dim_date["date_processing"]).dt.date
    fact["date_processing"]     = pd.to_datetime(fact["date_processing"]).dt.date
    fact["id_person"]           = fact["id_person"].astype(int)
    fact["id_act"]              = fact["id_act"].astype(int)
    fact["id_location"]         = fact["id_location"].astype(int)
    fact["total_victim"]        = fact["total_victim"].astype(int)

    conn = pymysql.connect(**DB_CONFIG)
    cur  = conn.cursor()

    # Create schema
    for ddl in DDL:
        cur.execute(ddl)
    conn.commit()
    print("Schema ready")

    # Insert dimensions first (FK order matters)
    _insert_dataframe(cur, "person",           dim_person)
    _insert_dataframe(cur, "victimizing_act",  dim_act)
    _insert_dataframe(cur, "location",         dim_location)
    _insert_dataframe(cur, "registration_date", dim_date)
    conn.commit()
    print("Dimensions loaded")

    # Insert fact table
    _insert_dataframe(cur, "victims", fact)
    conn.commit()
    print(f"Fact table loaded: {len(fact)} rows")

    conn.close()
    print("Load complete")
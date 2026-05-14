#import the necessary libraries
import pandas as pd
import requests
import os

def ingest_source3(url, start_year, output_path):
    """
    Ingest records from an API and saves them as a parquet file 

    Retrieves records from the API at "url" using a loop with 
    a limit of 50,000 rows per request. Each request filters the records by year
    (year >= start_year) and sorts them in descending order by year. The loop continues
    until the API returns an empty page or a partial page (with fewer rows than the
    page size), indicating that all available records have been retrieved.
    Label each row with "source3_api_displacement" for traceability and write
    the accumulated result to `output_path` in Parquet format.

    Args:
        url (str): Path to the source CSV file.
        start_year (int): the year from which the records will be retrieved
        output_path (str): Destination path for the output Parquet file.
    """

    all_records = []
    page_size = 50000
    offset = 0

    while True:
        params = {
            "$limit": page_size,
            "$offset": offset,
            "$where": f"vigencia >= '{start_year}'", 
            "$order": "vigencia DESC"  
        }
        try:
            response = requests.get(url, params=params, timeout=120)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise Exception(f"API request failed: {e}")

        records = response.json()

        if not records:
            break

        all_records.extend(records)
        print(f"Downloaded {len(all_records)} records so far...")

        if len(records) < page_size:
            break

        offset += page_size

    df = pd.DataFrame(all_records)
    df["source"] = "source3_api_displacement"

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)

    print(f"Source 3 ingested: {len(df)} records")
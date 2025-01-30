import pandas as pd
import numpy as np
from openai import OpenAI
import psycopg2
import json
import time
import sys
import os

client = OpenAI(
  api_key="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" # Tier 2 key
)

### Creating connection to the database
db_config = {
    "dbname": "reviews_db",
    "user": "scraping_user",
    "password": "Passwort123",
    "host": "localhost",
    "port": 5432
}

try:
    connection = psycopg2.connect(**db_config)
    cursor = connection.cursor()

except Exception as e:
    print(f"Fehler bei der Verbindung zur Datenbank: {str(e)}")
    sys.exit(1)


def create_review_batches(data, batch_size=45000):
    # Sort data by review_id
    data = data.sort_values(by="review_id").reset_index(drop=True)
    
    batches = []
    start_index = 0
    while start_index < len(data):
        start_review_id = data.loc[start_index, "review_id"]
        # Find the index where batch would exceed 45,000 reviews
        end_index = min(start_index + batch_size, len(data))
        # Adjust end_index to ensure we don't exceed batch_size and don't split a restaurant
        while (
            end_index < len(data) and 
            data.loc[end_index, "restaurant_id"] == data.loc[end_index - 1, "restaurant_id"]
        ):
            end_index -= 1  # Move back to avoid splitting the restaurant
        # Ensure at least one review in the batch
        if end_index <= start_index:
            end_index = start_index + 1  
        # Get the last review ID for the batch
        end_review_id = data.loc[end_index - 1, "review_id"]
        # Store batch info
        batches.append({"start_review_id": start_review_id, "end_review_id": end_review_id})
        # Move to next batch
        start_index = end_index  # Start from the next review

    return pd.DataFrame(batches)


def preprocess_reviews(data):
    data = data.dropna(subset=['review_text'])  # Drop rows with missing review_text
    data['review_text'] = data['review_text'].str.replace(r'\s+', ' ', regex=True).str.strip()  # Remove extra spaces, newlines, tabs
    data = data[data["review_text"].str.len() > 10]  # Keep only reviews longer than 10 characters
    return data




try:
    query_general = """
        SELECT review_id, restaurant_id
        FROM reviews_general
        LIMIT 90000;
        """

    cursor.execute(query_general)
    rows = cursor.fetchall()

    columns = [desc[0] for desc in cursor.description]

    data = pd.DataFrame(rows, columns=columns)
    length = len(data)
    print(f"length first information {length}")

except Exception as e:
    print(f"Failed to retrieve general information from the database {e}")
    sys.exit(1)     # exiting the file if data can't be retrieved


batches_protocol = create_review_batches(data, batch_size=45000)



for idx in range(0, len(batches_protocol), 2):  # Process two batches at a time
    # Extract the batch data using the batch protocol
    min_review_id_1 = batches_protocol.loc[idx, "start_review_id"]
    max_review_id_1 = batches_protocol.loc[idx, "end_review_id"]


    try:
        query_general_1 = f"""
            SELECT review_id, restaurant_id, review_text
            FROM reviews_general
            WHERE review_id between {min_review_id_1} AND {max_review_id_1};
            """

        cursor.execute(query_general_1)
        rows_1 = cursor.fetchall()

        columns_1 = [desc[0] for desc in cursor.description]

        data_1 = pd.DataFrame(rows_1, columns=columns_1)
        length_1 = len(data_1)
        print(f"Created data_1")
        print(f"length first information {length_1}")

    except Exception as e:
        print(f"Failed to retrieve data for batch 1 from the database, index {idx} {e}")
        sys.exit(1)     # exiting the file if data can't be retrieved


    if idx + 1 < len(batches_protocol):
        min_review_id_2 = batches_protocol.loc[idx + 1, "start_review_id"]
        max_review_id_2 = batches_protocol.loc[idx + 1, "end_review_id"]

        try:
            query_general_2 = f"""
                SELECT review_id, restaurant_id, review_text
                FROM reviews_general
                WHERE review_id between {min_review_id_2} AND {max_review_id_2};
                """

            cursor.execute(query_general_2)
            rows_2 = cursor.fetchall()

            columns_2 = [desc[0] for desc in cursor.description]

            data_2 = pd.DataFrame(rows_2, columns=columns_2)
            length_2 = len(data_2)
            print(f"Created data_2")
            print(f"length second information {length_2}")

        except Exception as e:
            print(f"Failed to retrieve data for batch 1 from the database, index {idx} {e}")
            sys.exit(1)     # exiting the file if data can't be retrieved
    else:
        data_2 = pd.DataFrame()  # In case there is no second batch, use an empty DataFrame




data_1 = preprocess_reviews(data_1)
if not data_2.empty:
    data_2 = preprocess_reviews(data_2)

data_1 = pd.DataFrame(data_1)
data_1.to_csv("/mnt/volume/backup_summaries/test.csv")


backup_dir = "/mnt/volume/backup_summaries"
os.makedirs(backup_dir, exist_ok=True)  # Erstellt das Verzeichnis, falls es nicht existiert
idx = 1

try:
    summaries_df = pd.DataFrame(data_1)
    backup_path = os.path.join(backup_dir, f"overall_summaries_{idx}.csv")
    summaries_df.to_csv(backup_path, index=False, encoding="utf-8")  # Setze UTF-8 Encoding
    print(f"Backup saved at {backup_path}")

except Exception as e:
    print(f"Failed to backup summaries on the server: {e}, {idx}")